import { Pool, TimeoutError } from "sequelize-pool";

import { NoActiveReadersError } from "./errors.js";
import { Semaphore } from "./semaphore.js";
import { FALLBACK_WRITE_CONNECTION, READER_POOL_KEY, READ_SLOT_HELD } from "./symbols.js";
import type { RdsReaderEndpoint, RdsReplicaBalancerLogger } from "./types.js";

export type QueryType = "read" | "write" | "SELECT" | string | undefined;

export interface PoolDelegate {
  acquire(...args: unknown[]): Promise<unknown>;
  release(connection: unknown): void;
  destroy(connection: unknown): Promise<void> | void;
  drain?(): Promise<unknown> | unknown;
  destroyAllNow?(): Promise<unknown> | unknown;
}

export interface DynamicReadPoolOptions {
  clusterIdentifier: string;
  writePool: PoolDelegate;
  poolOptions: {
    max: number;
    min?: number;
    idle?: number;
    acquire?: number;
    evict?: number;
    maxUses?: number;
  };
  connect(config: Record<string, unknown>): Promise<unknown>;
  disconnect(connection: unknown): Promise<unknown> | unknown;
  validate?: (connection: unknown) => boolean;
  fallbackToWriter: boolean;
  drainTimeoutMs: number;
  logger?: RdsReplicaBalancerLogger;
}

interface ReaderPoolEntry {
  key: string;
  endpoint: RdsReaderEndpoint;
  config: Record<string, unknown>;
  pool: Pool<unknown>;
  state: "active" | "draining";
}

export class DynamicReadPool {
  readonly #clusterIdentifier: string;
  readonly #writePool: PoolDelegate;
  readonly #poolOptions: DynamicReadPoolOptions["poolOptions"];
  readonly #connect: DynamicReadPoolOptions["connect"];
  readonly #disconnect: DynamicReadPoolOptions["disconnect"];
  readonly #validate: NonNullable<DynamicReadPoolOptions["validate"]>;
  readonly #fallbackToWriter: boolean;
  readonly #drainTimeoutMs: number;
  readonly #logger?: RdsReplicaBalancerLogger;
  readonly #readSlots: Semaphore;
  readonly #readers = new Map<string, ReaderPoolEntry>();
  #schedule: string[] = [];
  #cursor = 0;

  constructor(options: DynamicReadPoolOptions) {
    this.#clusterIdentifier = options.clusterIdentifier;
    this.#writePool = options.writePool;
    this.#poolOptions = options.poolOptions;
    this.#connect = options.connect;
    this.#disconnect = options.disconnect;
    this.#validate = options.validate ?? (() => true);
    this.#fallbackToWriter = options.fallbackToWriter;
    this.#drainTimeoutMs = options.drainTimeoutMs;
    this.#logger = options.logger;
    this.#readSlots = new Semaphore(Math.max(1, options.poolOptions.max));
  }

  updateReaders(readers: Array<{ endpoint: RdsReaderEndpoint; config: Record<string, unknown> }>): void {
    const nextKeys = new Set<string>();

    for (const reader of readers) {
      const key = readerKey(reader.endpoint);
      nextKeys.add(key);
      const existing = this.#readers.get(key);

      if (existing?.state === "active") {
        existing.endpoint = reader.endpoint;
        existing.config = reader.config;
        continue;
      }

      this.#readers.set(key, {
        key,
        endpoint: reader.endpoint,
        config: reader.config,
        state: "active",
        pool: this.#createPool(key, reader.config),
      });
      this.#logger?.info?.("Added RDS reader to Sequelize read pool.", {
        instanceIdentifier: reader.endpoint.instanceIdentifier,
        host: reader.endpoint.host,
        port: reader.endpoint.port,
      });
    }

    for (const [key, entry] of this.#readers) {
      if (!nextKeys.has(key) && entry.state === "active") {
        this.#drainReader(entry);
      }
    }

    this.#schedule = [...nextKeys].filter((key) => this.#readers.get(key)?.state === "active");
    this.#cursor %= Math.max(1, this.#schedule.length);
  }

  async acquireRead(): Promise<unknown> {
    if (this.#schedule.length === 0) {
      if (this.#fallbackToWriter) {
        const connection = await this.#writePool.acquire("SELECT", true);
        tagConnection(connection, FALLBACK_WRITE_CONNECTION, true);
        return connection;
      }

      throw new NoActiveReadersError(this.#clusterIdentifier);
    }

    await this.#readSlots.acquire();

    try {
      const entry = this.#nextReader();
      const connection = await entry.pool.acquire();
      tagConnection(connection, READER_POOL_KEY, entry.key);
      tagConnection(connection, READ_SLOT_HELD, true);
      return connection;
    } catch (error) {
      this.#readSlots.release();

      if (error instanceof TimeoutError) {
        throw error;
      }

      throw error;
    }
  }

  release(connection: unknown): boolean {
    if (getConnectionTag(connection, FALLBACK_WRITE_CONNECTION)) {
      clearConnectionTag(connection, FALLBACK_WRITE_CONNECTION);
      this.#writePool.release(connection);
      return true;
    }

    const key = getConnectionTag(connection, READER_POOL_KEY);

    if (typeof key !== "string") {
      return false;
    }

    const entry = this.#readers.get(key);

    if (!entry) {
      this.#releaseReadSlot(connection);
      return false;
    }

    entry.pool.release(connection);
    this.#releaseReadSlot(connection);
    return true;
  }

  async destroy(connection: unknown): Promise<boolean> {
    if (getConnectionTag(connection, FALLBACK_WRITE_CONNECTION)) {
      clearConnectionTag(connection, FALLBACK_WRITE_CONNECTION);
      await this.#writePool.destroy(connection);
      return true;
    }

    const key = getConnectionTag(connection, READER_POOL_KEY);

    if (typeof key !== "string") {
      return false;
    }

    const entry = this.#readers.get(key);

    if (!entry) {
      this.#releaseReadSlot(connection);
      return false;
    }

    await entry.pool.destroy(connection);
    this.#releaseReadSlot(connection);
    return true;
  }

  async drain(): Promise<void> {
    await Promise.all([...this.#readers.values()].map((entry) => entry.pool.drain()));
  }

  async destroyAllNow(): Promise<void> {
    await Promise.all([...this.#readers.values()].map((entry) => entry.pool.destroyAllNow()));
    this.#readers.clear();
    this.#schedule = [];
  }

  get size(): number {
    return sumPools(this.#readers, "size");
  }

  get available(): number {
    return sumPools(this.#readers, "available");
  }

  get using(): number {
    return sumPools(this.#readers, "using");
  }

  get waiting(): number {
    return sumPools(this.#readers, "waiting") + this.#readSlots.waiting;
  }

  get readerCount(): number {
    return this.#schedule.length;
  }

  #nextReader(): ReaderPoolEntry {
    for (let attempts = 0; attempts < this.#schedule.length; attempts += 1) {
      const key = this.#schedule[this.#cursor % this.#schedule.length];
      this.#cursor += 1;
      const entry = this.#readers.get(key);

      if (entry?.state === "active") {
        return entry;
      }
    }

    throw new NoActiveReadersError(this.#clusterIdentifier);
  }

  #createPool(key: string, config: Record<string, unknown>): Pool<unknown> {
    return new Pool({
      name: `sequelize-rds:read:${key}`,
      create: () => this.#connect(config),
      destroy: async (connection) => {
        await this.#disconnect(connection);
      },
      validate: this.#validate,
      max: this.#poolOptions.max,
      min: this.#poolOptions.min ?? 0,
      acquireTimeoutMillis: this.#poolOptions.acquire ?? 60_000,
      idleTimeoutMillis: this.#poolOptions.idle ?? 10_000,
      reapIntervalMillis: this.#poolOptions.evict ?? 1_000,
      maxUses: this.#poolOptions.maxUses,
    });
  }

  #drainReader(entry: ReaderPoolEntry): void {
    entry.state = "draining";
    this.#logger?.info?.("Draining removed RDS reader from Sequelize read pool.", {
      instanceIdentifier: entry.endpoint.instanceIdentifier,
      host: entry.endpoint.host,
      port: entry.endpoint.port,
    });

    void drainAndDestroy(entry, this.#drainTimeoutMs, this.#logger).finally(() => {
      this.#readers.delete(entry.key);
    });
  }

  #releaseReadSlot(connection: unknown): void {
    if (!getConnectionTag(connection, READ_SLOT_HELD)) {
      return;
    }

    clearConnectionTag(connection, READ_SLOT_HELD);
    this.#readSlots.release();
  }
}

export function readerKey(endpoint: RdsReaderEndpoint): string {
  return `${endpoint.instanceIdentifier}:${endpoint.host}:${endpoint.port}`;
}

async function drainAndDestroy(
  entry: ReaderPoolEntry,
  drainTimeoutMs: number,
  logger?: RdsReplicaBalancerLogger,
): Promise<void> {
  let timeout: NodeJS.Timeout | undefined;

  try {
    await Promise.race([
      entry.pool.drain(),
      new Promise<void>((resolve) => {
        timeout = setTimeout(resolve, drainTimeoutMs);
      }),
    ]);
    await entry.pool.destroyAllNow();
  } catch (error) {
    logger?.warn?.("Failed while draining RDS reader pool.", {
      instanceIdentifier: entry.endpoint.instanceIdentifier,
      error,
    });
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

function sumPools(readers: Map<string, ReaderPoolEntry>, property: "size" | "available" | "using" | "waiting"): number {
  let total = 0;

  for (const entry of readers.values()) {
    total += Number(entry.pool[property] ?? 0);
  }

  return total;
}

function tagConnection(connection: unknown, symbol: symbol, value: unknown): void {
  if ((typeof connection !== "object" && typeof connection !== "function") || connection === null) {
    return;
  }

  Object.defineProperty(connection, symbol, {
    configurable: true,
    enumerable: false,
    writable: true,
    value,
  });
}

function getConnectionTag(connection: unknown, symbol: symbol): unknown {
  if ((typeof connection !== "object" && typeof connection !== "function") || connection === null) {
    return undefined;
  }

  return (connection as Record<symbol, unknown>)[symbol];
}

function clearConnectionTag(connection: unknown, symbol: symbol): void {
  if ((typeof connection !== "object" && typeof connection !== "function") || connection === null) {
    return;
  }

  delete (connection as Record<symbol, unknown>)[symbol];
}
