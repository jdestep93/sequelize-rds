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
  if (sequelize.connectionManager?.pool) {
    return installV6Adapter({ sequelize, options });
  }

  if (sequelize.pool) {
    return installV7Adapter({ sequelize, options });
  }

  throw new RdsReplicaBalancerError("Unable to find a Sequelize connection pool to wrap.");
}

function installV6Adapter(context: AdapterContext): SequelizePoolAdapter {
  const connectionManager = context.sequelize.connectionManager;

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
  const originalPool = context.sequelize.pool;

  if (!originalPool) {
    throw new RdsReplicaBalancerError("Sequelize v7 pool was not found.");
  }

  const poolDelegate = originalPool as PoolDelegate & Record<string, unknown>;
  const dynamicReads = createDynamicReadPool(context, poolDelegate);
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
  const source = objectAt(sequelize.connectionManager?.config, "pool") ?? objectAt(sequelize.options, "pool") ?? {};

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
  const config = sequelize.connectionManager?.config ?? sequelize.config ?? {};
  const options = sequelize.options ?? {};
  const replication = objectAt(config, "replication") ?? objectAt(options, "replication");
  const read = Array.isArray(replication.read) ? replication.read[0] : replication.read;

  return {
    ...configWithoutReplication(config),
    ...configWithoutReplication(options),
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
    sequelize.connectionManager?.config,
    sequelize.config,
    sequelize.options,
    objectAt(sequelize.connectionManager?.config, "replication")?.write,
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
  const manager = sequelize.connectionManager as
    | {
        _connect?: (config: Record<string, unknown>) => Promise<unknown>;
      }
    | undefined;

  if (manager?._connect) {
    return manager._connect(config);
  }

  const anySequelize = sequelize as Record<string, any>;
  const clonedConfig = { ...config };

  await anySequelize.hooks?.runAsync?.("beforeConnect", clonedConfig);
  await anySequelize.runHooks?.("beforeConnect", clonedConfig);
  const connection = await anySequelize.dialect?.connectionManager?.connect?.(clonedConfig);

  if (!connection) {
    throw new RdsReplicaBalancerError("Unable to create a Sequelize connection for an RDS reader.");
  }

  await anySequelize.hooks?.runAsync?.("afterConnect", connection, clonedConfig);
  await anySequelize.runHooks?.("afterConnect", connection, clonedConfig);
  return connection;
}

async function disconnect(sequelize: SequelizeLike, connection: unknown): Promise<unknown> {
  const manager = sequelize.connectionManager as
    | {
        _disconnect?: (connection: unknown) => Promise<unknown>;
      }
    | undefined;

  if (manager?._disconnect) {
    return manager._disconnect(connection);
  }

  const anySequelize = sequelize as Record<string, any>;
  await anySequelize.hooks?.runAsync?.("beforeDisconnect", connection);
  await anySequelize.runHooks?.("beforeDisconnect", connection);
  const result = await anySequelize.dialect?.connectionManager?.disconnect?.(connection);
  await anySequelize.hooks?.runAsync?.("afterDisconnect", connection);
  await anySequelize.runHooks?.("afterDisconnect", connection);
  return result;
}

function validate(sequelize: SequelizeLike, connection: unknown): boolean {
  const anySequelize = sequelize as Record<string, any>;
  const validator =
    anySequelize.connectionManager?.dialect?.connectionManager?.validate ??
    anySequelize.dialect?.connectionManager?.validate;

  return validator ? Boolean(validator(connection)) : true;
}

function configWithoutReplication(config: Record<string, unknown>): Record<string, unknown> {
  const { replication: _replication, pool: _pool, ...rest } = config;
  return rest;
}

function objectAt(value: unknown, key?: string): Record<string, any> {
  if ((typeof value !== "object" && typeof value !== "function") || value === null) {
    return {};
  }

  if (!key) {
    return value as Record<string, any>;
  }

  const child = (value as Record<string, unknown>)[key];

  if ((typeof child !== "object" && typeof child !== "function") || child === null) {
    return {};
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
