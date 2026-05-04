import { DynamicReadPool, type PoolDelegate } from "./dynamic-read-pool.js";
import { RdsReplicaBalancerError } from "./errors.js";
import type {
  RdsClusterTopology,
  RdsReaderEndpoint,
  RdsReplicaBalancerLogger,
  RdsReplicaBalancerOptions,
  SequelizeLike,
} from "./types.js";

export interface SequelizePoolAdapter {
  applyTopology(topology: RdsClusterTopology): void;
  destroy(): Promise<void>;
}

interface AdapterContext {
  sequelize: SequelizeLike;
  options: Required<
    Pick<RdsReplicaBalancerOptions, "clusterIdentifier" | "drainTimeoutMs" | "fallbackToWriter" | "keepExistingWriteHost">
  > &
    Pick<RdsReplicaBalancerOptions, "logger">;
}

export function installSequelizePoolAdapter(sequelize: SequelizeLike, options: AdapterContext["options"]): SequelizePoolAdapter {
  if (safeGet(sequelize, "pool")) {
    return installV7Adapter({ sequelize, options });
  }

  if (safeConnectionManager(sequelize)?.pool) {
    return installV6Adapter({ sequelize, options });
  }

  throw new RdsReplicaBalancerError("Unable to find a Sequelize connection pool to wrap.");
}

function installV6Adapter(context: AdapterContext): SequelizePoolAdapter {
  const connectionManager = safeConnectionManager(context.sequelize);

  if (!connectionManager?.pool) {
    throw new RdsReplicaBalancerError("Sequelize v6 connectionManager.pool was not found.");
  }

  const originalPool = connectionManager.pool as PoolDelegate & Record<string, unknown>;
  const dynamicReads = createDynamicReadPool(context, originalPool);
  const facade = new V6PoolFacade(originalPool, dynamicReads);
  connectionManager.pool = facade;

  return {
    applyTopology(topology) {
      updateWriteHost(context.sequelize, topology, context.options);
      dynamicReads.updateReaders(readersToConfigs(context.sequelize, topology.readers));
    },
    async destroy() {
      connectionManager.pool = originalPool;
      await dynamicReads.destroyAllNow();
    },
  };
}

function installV7Adapter(context: AdapterContext): SequelizePoolAdapter {
  const originalPool = safeGet(context.sequelize, "pool");

  if (!originalPool) {
    throw new RdsReplicaBalancerError("Sequelize v7 pool was not found.");
  }

  const poolDelegate = originalPool as PoolDelegate & Record<string, unknown>;
  const dynamicReads = createDynamicReadPool(context, createV7WriteFallbackDelegate(poolDelegate));
  const facade = new V7PoolFacade(poolDelegate, dynamicReads);
  context.sequelize.pool = facade;

  return {
    applyTopology(topology) {
      updateWriteHost(context.sequelize, topology, context.options);
      dynamicReads.updateReaders(readersToConfigs(context.sequelize, topology.readers));
    },
    async destroy() {
      context.sequelize.pool = originalPool;
      await dynamicReads.destroyAllNow();
    },
  };
}

class V6PoolFacade implements PoolDelegate {
  read: DynamicReadPool;
  write: unknown;

  constructor(
    private readonly originalPool: PoolDelegate & Record<string, unknown>,
    private readonly dynamicReads: DynamicReadPool,
  ) {
    this.read = dynamicReads;
    this.write = originalPool.write ?? originalPool;
  }

  acquire(queryType?: unknown, useMaster?: unknown): Promise<unknown> {
    if (queryType === "SELECT" && !useMaster) {
      return this.dynamicReads.acquireRead();
    }

    return this.originalPool.acquire(queryType, useMaster);
  }

  release(connection: unknown): void {
    if (!this.dynamicReads.release(connection)) {
      this.originalPool.release(connection);
    }
  }

  async destroy(connection: unknown): Promise<void> {
    if (!(await this.dynamicReads.destroy(connection))) {
      await this.originalPool.destroy(connection);
    }
  }

  async drain(): Promise<void> {
    await Promise.all([this.dynamicReads.drain(), this.originalPool.drain?.()]);
  }

  async destroyAllNow(): Promise<void> {
    await Promise.all([this.dynamicReads.destroyAllNow(), this.originalPool.destroyAllNow?.()]);
  }

  get size(): number {
    return this.dynamicReads.size + numericProperty(this.write, "size");
  }

  get available(): number {
    return this.dynamicReads.available + numericProperty(this.write, "available");
  }

  get using(): number {
    return this.dynamicReads.using + numericProperty(this.write, "using");
  }

  get waiting(): number {
    return this.dynamicReads.waiting + numericProperty(this.write, "waiting");
  }
}

class V7PoolFacade implements PoolDelegate {
  read: DynamicReadPool;
  write: unknown;

  constructor(
    private readonly originalPool: PoolDelegate & Record<string, unknown>,
    private readonly dynamicReads: DynamicReadPool,
  ) {
    this.read = dynamicReads;
    this.write = getOriginalPool(originalPool, "write");
  }

  acquire(options?: { type?: string; useMaster?: boolean } | undefined): Promise<unknown> {
    if (options?.type === "read" && !options.useMaster) {
      return this.dynamicReads.acquireRead();
    }

    return this.originalPool.acquire(options);
  }

  release(connection: unknown): void {
    if (!this.dynamicReads.release(connection)) {
      this.originalPool.release(connection);
    }
  }

  async destroy(connection: unknown): Promise<void> {
    if (!(await this.dynamicReads.destroy(connection))) {
      await this.originalPool.destroy(connection);
    }
  }

  async destroyAllNow(): Promise<void> {
    await Promise.all([this.dynamicReads.destroyAllNow(), this.originalPool.destroyAllNow?.()]);
  }

  async drain(): Promise<void> {
    await Promise.all([this.dynamicReads.drain(), this.originalPool.drain?.()]);
  }

  getPool(poolType: "read" | "write"): unknown {
    if (poolType === "read") {
      return this.dynamicReads;
    }

    return getOriginalPool(this.originalPool, "write");
  }

  get size(): number {
    return this.dynamicReads.size + numericProperty(this.write, "size");
  }

  get available(): number {
    return this.dynamicReads.available + numericProperty(this.write, "available");
  }

  get using(): number {
    return this.dynamicReads.using + numericProperty(this.write, "using");
  }

  get waiting(): number {
    return this.dynamicReads.waiting + numericProperty(this.write, "waiting");
  }
}

function createDynamicReadPool(context: AdapterContext, writePool: PoolDelegate): DynamicReadPool {
  const poolOptions = getPoolOptions(context.sequelize);

  return new DynamicReadPool({
    clusterIdentifier: context.options.clusterIdentifier,
    writePool,
    poolOptions,
    fallbackToWriter: context.options.fallbackToWriter,
    drainTimeoutMs: context.options.drainTimeoutMs,
    logger: context.options.logger,
    connect: (config) => connect(context.sequelize, config),
    disconnect: (connection) => disconnect(context.sequelize, connection),
    validate: (connection) => validate(context.sequelize, connection),
  });
}

function createV7WriteFallbackDelegate(originalPool: PoolDelegate & Record<string, unknown>): PoolDelegate {
  return {
    acquire: () => originalPool.acquire({ type: "write", useMaster: true }),
    release: (connection) => originalPool.release(connection),
    destroy: (connection) => originalPool.destroy(connection),
    drain: () => originalPool.drain?.(),
    destroyAllNow: () => originalPool.destroyAllNow?.(),
  };
}

function readersToConfigs(
  sequelize: SequelizeLike,
  readers: RdsReaderEndpoint[],
): Array<{ endpoint: RdsReaderEndpoint; config: Record<string, unknown> }> {
  return readers.map((endpoint) => ({
    endpoint,
    config: {
      ...getBaseReadConfig(sequelize),
      host: endpoint.host,
      port: endpoint.port,
    },
  }));
}

function getPoolOptions(sequelize: SequelizeLike): {
  max: number;
  min: number;
  idle: number;
  acquire: number;
  evict: number;
  maxUses?: number;
} {
  const source = objectAt(safeConnectionManager(sequelize)?.config, "pool") ?? objectAt(sequelize.options, "pool") ?? {};

  return {
    max: numberOrDefault(source.max, 5),
    min: numberOrDefault(source.min, 0),
    idle: numberOrDefault(source.idle, 10_000),
    acquire: numberOrDefault(source.acquire, 60_000),
    evict: numberOrDefault(source.evict, 1_000),
    maxUses: typeof source.maxUses === "number" ? source.maxUses : undefined,
  };
}

function getBaseReadConfig(sequelize: SequelizeLike): Record<string, unknown> {
  const config = safeConnectionManager(sequelize)?.config ?? sequelize.config ?? {};
  const options = sequelize.options ?? {};
  const replication = objectAt(config, "replication") ?? objectAt(options, "replication");
  const read = Array.isArray(replication?.read) ? replication.read[0] : replication?.read;

  return {
    ...configWithoutReplication(config),
    ...(replication ? {} : connectionOptionsFrom(options)),
    ...objectAt(replication, "write"),
    ...objectAt(read),
  };
}

function updateWriteHost(
  sequelize: SequelizeLike,
  topology: RdsClusterTopology,
  options: Pick<RdsReplicaBalancerOptions, "keepExistingWriteHost" | "logger">,
): void {
  if (options.keepExistingWriteHost || !topology.writer) {
    return;
  }

  const targets = [
    safeConnectionManager(sequelize)?.config,
    sequelize.config,
    sequelize.options,
    objectAt(safeConnectionManager(sequelize)?.config, "replication")?.write,
    objectAt(sequelize.options, "replication")?.write,
  ];

  for (const target of targets) {
    if (!target) {
      continue;
    }

    target.host = topology.writer.host;
    target.port = topology.writer.port;
  }

  options.logger?.debug?.("Updated Sequelize writer endpoint from RDS topology.", {
    host: topology.writer.host,
    port: topology.writer.port,
  });
}

async function connect(sequelize: SequelizeLike, config: Record<string, unknown>): Promise<unknown> {
  const manager = safeConnectionManager(sequelize) as
    | {
        _connect?: (config: Record<string, unknown>) => Promise<unknown>;
      }
    | undefined;

  if (manager?._connect) {
    return manager._connect(config);
  }

  const anySequelize = sequelize as Record<string, any>;
  const clonedConfig = { ...config };

  await runSequelizeHook(anySequelize, "beforeConnect", clonedConfig);
  const connection = await anySequelize.dialect?.connectionManager?.connect?.(clonedConfig);

  if (!connection) {
    throw new RdsReplicaBalancerError("Unable to create a Sequelize connection for an RDS reader.");
  }

  await runSequelizeHook(anySequelize, "afterConnect", connection, clonedConfig);
  return connection;
}

async function disconnect(sequelize: SequelizeLike, connection: unknown): Promise<unknown> {
  const manager = safeConnectionManager(sequelize) as
    | {
        _disconnect?: (connection: unknown) => Promise<unknown>;
      }
    | undefined;

  if (manager?._disconnect) {
    return manager._disconnect(connection);
  }

  const anySequelize = sequelize as Record<string, any>;
  await runSequelizeHook(anySequelize, "beforeDisconnect", connection);
  const result = await anySequelize.dialect?.connectionManager?.disconnect?.(connection);
  await runSequelizeHook(anySequelize, "afterDisconnect", connection);
  return result;
}

function validate(sequelize: SequelizeLike, connection: unknown): boolean {
  const anySequelize = sequelize as Record<string, any>;
  const validator =
    safeConnectionManager(sequelize)?.dialect?.connectionManager?.validate ??
    anySequelize.dialect?.connectionManager?.validate;

  return validator ? Boolean(validator(connection)) : true;
}

function configWithoutReplication(config: Record<string, unknown> | undefined): Record<string, unknown> {
  if (!config) {
    return {};
  }

  const { replication: _replication, pool: _pool, ...rest } = config;
  return rest;
}

function connectionOptionsFrom(config: Record<string, unknown> | undefined): Record<string, unknown> {
  if (!config) {
    return {};
  }

  const allowedKeys = [
    "database",
    "username",
    "user",
    "password",
    "host",
    "port",
    "dialect",
    "ssl",
    "dialectModule",
    "pgModule",
    "mysql2Module",
  ];
  const result: Record<string, unknown> = {};

  for (const key of allowedKeys) {
    if (key in config) {
      result[key] = config[key];
    }
  }

  return result;
}

function objectAt(value: unknown, key?: string): Record<string, any> | undefined {
  if ((typeof value !== "object" && typeof value !== "function") || value === null) {
    return undefined;
  }

  if (!key) {
    return value as Record<string, any>;
  }

  const child = (value as Record<string, unknown>)[key];

  if ((typeof child !== "object" && typeof child !== "function") || child === null) {
    return undefined;
  }

  return child as Record<string, any>;
}

function numberOrDefault(value: unknown, fallback: number): number {
  return typeof value === "number" ? value : fallback;
}

function getOriginalPool(originalPool: Record<string, unknown>, type: "read" | "write"): unknown {
  const getPool = originalPool.getPool;

  if (typeof getPool === "function") {
    return getPool.call(originalPool, type);
  }

  return originalPool[type] ?? originalPool;
}

function numericProperty(target: unknown, property: string): number {
  if ((typeof target !== "object" && typeof target !== "function") || target === null) {
    return 0;
  }

  const value = (target as Record<string, unknown>)[property];
  return typeof value === "number" ? value : 0;
}

async function runSequelizeHook(sequelize: Record<string, any>, name: string, ...args: unknown[]): Promise<void> {
  if (sequelize.hooks?.runAsync) {
    await sequelize.hooks.runAsync(name, ...args);
    return;
  }

  await sequelize.runHooks?.(name, ...args);
}

function safeConnectionManager(sequelize: SequelizeLike): any {
  return safeGet(sequelize, "connectionManager");
}

function safeGet<T extends object, K extends PropertyKey>(target: T | undefined, key: K): any {
  if (!target) {
    return undefined;
  }

  try {
    return (target as Record<K, unknown>)[key];
  } catch {
    return undefined;
  }
}
