import {
  DescribeDBClustersCommand,
  DescribeDBInstancesCommand,
  type DBCluster,
  type DBInstance,
} from "@aws-sdk/client-rds";
import { vi } from "vitest";

import type { RdsClientLike } from "../src/index.js";

export function createRdsClientSequence(
  snapshots: Array<{ cluster: DBCluster; instances: DBInstance[] }>,
): RdsClientLike {
  let syncIndex = 0;
  let phase: "cluster" | "instances" = "cluster";
  let pendingInstances = new Set<string>();

  return {
    async send(command) {
      const snapshot = snapshots[Math.min(syncIndex, snapshots.length - 1)];

      if (command instanceof DescribeDBClustersCommand) {
        phase = "instances";
        pendingInstances = new Set(
          (snapshot.cluster.DBClusterMembers ?? [])
            .map((member) => member.DBInstanceIdentifier)
            .filter((identifier): identifier is string => Boolean(identifier)),
        );
        return { DBClusters: [snapshot.cluster] };
      }

      if (command instanceof DescribeDBInstancesCommand) {
        if (phase !== "instances") {
          throw new Error("DescribeDBInstances called before DescribeDBClusters");
        }

        const identifier = command.input.DBInstanceIdentifier;
        const instance = snapshot.instances.find((candidate) => candidate.DBInstanceIdentifier === identifier);

        if (identifier) {
          pendingInstances.delete(identifier);
        }

        if (pendingInstances.size === 0) {
          phase = "cluster";
          syncIndex += 1;
        }

        return { DBInstances: instance ? [instance] : [] };
      }

      throw new Error("Unexpected RDS command");
    },
  };
}

export function cluster(members: DBCluster["DBClusterMembers"]): DBCluster {
  return {
    DBClusterIdentifier: "app-cluster",
    Endpoint: "writer.cluster.local",
    ReaderEndpoint: "reader.cluster.local",
    Port: 5432,
    DBClusterMembers: members,
  };
}

export function instance(id: string, status = "available", address = `${id}.local`): DBInstance {
  return {
    DBInstanceIdentifier: id,
    DBInstanceStatus: status,
    Endpoint: address ? { Address: address, Port: 5432 } : undefined,
  };
}

export class FakePool {
  readonly acquired: unknown[] = [];
  readonly released: unknown[] = [];
  readonly destroyed: unknown[] = [];
  drainCalls = 0;
  destroyAllNowCalls = 0;
  nextId = 0;

  async acquire(...args: unknown[]): Promise<unknown> {
    const connection = { kind: "write", id: this.nextId, args };
    this.nextId += 1;
    this.acquired.push(connection);
    return connection;
  }

  release(connection: unknown): void {
    this.released.push(connection);
  }

  async destroy(connection: unknown): Promise<void> {
    this.destroyed.push(connection);
  }

  async drain(): Promise<void> {
    this.drainCalls += 1;
  }

  async destroyAllNow(): Promise<void> {
    this.destroyAllNowCalls += 1;
  }

  get size(): number {
    return this.acquired.length;
  }

  get available(): number {
    return 0;
  }

  get using(): number {
    return 0;
  }

  get waiting(): number {
    return 0;
  }
}

export function createFakeV6Sequelize(writePool = new FakePool()) {
  const connected: Array<Record<string, unknown>> = [];
  const disconnected: unknown[] = [];

  const sequelize = {
    connectionManager: {
      pool: writePool,
      config: {
        database: "app",
        username: "user",
        password: "secret",
        dialect: "postgres",
        pool: { max: 1, min: 0, idle: 100, acquire: 200, evict: 50 },
        replication: {
          write: { host: "old-writer.local", port: 5432 },
          read: [],
        },
      },
      _connect: vi.fn(async (config: Record<string, unknown>) => {
        const connection = { id: connected.length, host: config.host, port: config.port };
        connected.push(connection);
        return connection;
      }),
      _disconnect: vi.fn(async (connection: unknown) => {
        disconnected.push(connection);
      }),
    },
  };

  return { sequelize, writePool, connected, disconnected };
}

export function createFakeV7Sequelize(writePool = new FakePool()) {
  const connected: Array<Record<string, unknown>> = [];
  const disconnected: unknown[] = [];

  const sequelize = {
    pool: writePool,
    options: {
      database: "app",
      username: "user",
      password: "secret",
      dialect: "postgres",
      pool: { max: 1, min: 0, idle: 100, acquire: 200, evict: 50 },
      replication: {
        write: { host: "old-writer.local", port: 5432 },
        read: [],
      },
    },
    hooks: {
      runAsync: vi.fn(async () => {}),
    },
    dialect: {
      connectionManager: {
        connect: vi.fn(async (config: Record<string, unknown>) => {
          const connection = { id: connected.length, host: config.host, port: config.port };
          connected.push(connection);
          return connection;
        }),
        disconnect: vi.fn(async (connection: unknown) => {
          disconnected.push(connection);
        }),
        validate: vi.fn(() => true),
      },
    },
  };

  return { sequelize, writePool, connected, disconnected };
}

export async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
