import { describe, expect, it, vi } from "vitest";

import { NoActiveReadersError, attachRdsReplicaBalancer, createRdsAwareSequelize } from "../src/index.js";
import {
  cluster,
  createFakeV6Sequelize,
  createFakeV7Sequelize,
  createRdsClientSequence,
  instance,
  sleep,
} from "./helpers.js";

describe("RdsReplicaBalancerController", () => {
  it("routes v6 read traffic to discovered readers and updates the writer endpoint", async () => {
    const { sequelize, connected } = createFakeV6Sequelize();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-1")],
        },
      ]),
      fallbackToWriter: false,
    });

    await balancer.syncNow();
    const connection = await sequelize.connectionManager.pool.acquire("SELECT", false);

    expect(connection).toMatchObject({ host: "reader-1.local", port: 5432 });
    expect(connected).toHaveLength(1);
    expect(sequelize.connectionManager.config.replication.write.host).toBe("writer.cluster.local");

    sequelize.connectionManager.pool.release(connection);
    await balancer.destroy();
  });

  it("stops scheduling removed v6 readers while allowing in-flight connections to release", async () => {
    const { sequelize } = createFakeV6Sequelize();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-1")],
        },
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-1", "deleting")],
        },
      ]),
      fallbackToWriter: false,
      drainTimeoutMs: 10,
    });

    await balancer.syncNow();
    const inFlight = await sequelize.connectionManager.pool.acquire("SELECT", false);
    await balancer.syncNow();

    await expect(sequelize.connectionManager.pool.acquire("SELECT", false)).rejects.toBeInstanceOf(NoActiveReadersError);
    expect(() => sequelize.connectionManager.pool.release(inFlight)).not.toThrow();

    await balancer.destroy();
  });

  it("falls back to the writer when no readers are active and fallback is enabled", async () => {
    const { sequelize, writePool } = createFakeV6Sequelize();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([{ DBInstanceIdentifier: "writer-1", IsClusterWriter: true }]),
          instances: [instance("writer-1")],
        },
      ]),
      fallbackToWriter: true,
    });

    await balancer.syncNow();
    const connection = await sequelize.connectionManager.pool.acquire("SELECT", false);

    expect(connection).toMatchObject({ kind: "write" });
    expect(writePool.acquired).toHaveLength(1);

    sequelize.connectionManager.pool.release(connection);
    expect(writePool.released).toHaveLength(1);
    await balancer.destroy();
  });

  it("enforces a global read concurrency cap across reader pools", async () => {
    const { sequelize } = createFakeV6Sequelize();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
            { DBInstanceIdentifier: "reader-2", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-1"), instance("reader-2")],
        },
      ]),
      fallbackToWriter: false,
    });

    await balancer.syncNow();
    const first = await sequelize.connectionManager.pool.acquire("SELECT", false);
    let secondResolved = false;
    const secondPromise = sequelize.connectionManager.pool.acquire("SELECT", false).then((connection: unknown) => {
      secondResolved = true;
      return connection;
    });

    await sleep(20);
    expect(secondResolved).toBe(false);

    sequelize.connectionManager.pool.release(first);
    const second = await secondPromise;

    expect(secondResolved).toBe(true);
    sequelize.connectionManager.pool.release(second);
    await balancer.destroy();
  });

  it("wraps the v7 pool acquire options shape", async () => {
    const { sequelize } = createFakeV7Sequelize();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-1")],
        },
      ]),
      fallbackToWriter: false,
    });

    await balancer.syncNow();
    const readConnection = await sequelize.pool.acquire({ type: "read" });
    const writeConnection = await sequelize.pool.acquire({ type: "write" });

    expect(readConnection).toMatchObject({ host: "reader-1.local" });
    expect(writeConnection).toMatchObject({ kind: "write" });

    sequelize.pool.release(readConnection);
    sequelize.pool.release(writeConnection);
    await balancer.destroy();
  });

  it("round-robins reads across active readers when pool capacity allows it", async () => {
    const { sequelize } = createFakeV6Sequelize();
    sequelize.connectionManager.config.pool.max = 2;
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
            { DBInstanceIdentifier: "reader-2", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-1"), instance("reader-2")],
        },
      ]),
      fallbackToWriter: false,
    });

    await balancer.syncNow();
    const first = await sequelize.connectionManager.pool.acquire("SELECT", false);
    const second = await sequelize.connectionManager.pool.acquire("SELECT", false);

    expect([hostOf(first), hostOf(second)]).toEqual(["reader-1.local", "reader-2.local"]);

    sequelize.connectionManager.pool.release(first);
    sequelize.connectionManager.pool.release(second);
    await balancer.destroy();
  });

  it("does not mutate the Sequelize writer host when keepExistingWriteHost is enabled", async () => {
    const { sequelize } = createFakeV6Sequelize();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-1")],
        },
      ]),
      keepExistingWriteHost: true,
    });

    await balancer.syncNow();

    expect(sequelize.connectionManager.config.replication.write.host).toBe("old-writer.local");
    await balancer.destroy();
  });

  it("calls onTopologyChange only when the routed topology signature changes", async () => {
    const { sequelize } = createFakeV6Sequelize();
    const onTopologyChange = vi.fn();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-1")],
        },
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-1")],
        },
        {
          cluster: cluster([
            { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
            { DBInstanceIdentifier: "reader-2", IsClusterWriter: false },
          ]),
          instances: [instance("writer-1"), instance("reader-2")],
        },
      ]),
      onTopologyChange,
    });

    await balancer.syncNow();
    await balancer.syncNow();
    await balancer.syncNow();

    expect(onTopologyChange).toHaveBeenCalledTimes(2);
    expect(balancer.getTopology()?.readers[0]?.instanceIdentifier).toBe("reader-2");
    await balancer.destroy();
  });

  it("coalesces concurrent syncNow calls into one RDS discovery", async () => {
    const { sequelize } = createFakeV6Sequelize();
    let clusterCalls = 0;
    let releaseCluster!: () => void;
    const clusterGate = new Promise<void>((resolve) => {
      releaseCluster = resolve;
    });
    const rdsClient = {
      async send(command: unknown) {
        const { DescribeDBClustersCommand, DescribeDBInstancesCommand } = await import("@aws-sdk/client-rds");

        if (command instanceof DescribeDBClustersCommand) {
          clusterCalls += 1;
          await clusterGate;
          return {
            DBClusters: [
              cluster([
                { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
                { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
              ]),
            ],
          };
        }

        if (command instanceof DescribeDBInstancesCommand) {
          return { DBInstances: [instance("writer-1"), instance("reader-1")] };
        }

        throw new Error("Unexpected command");
      },
    };
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient,
    });

    const first = balancer.syncNow();
    const second = balancer.syncNow();
    releaseCluster();
    await Promise.all([first, second]);

    expect(clusterCalls).toBe(1);
    await balancer.destroy();
  });

  it("restores the original v6 pool on destroy", async () => {
    const { sequelize, writePool } = createFakeV6Sequelize();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([{ DBInstanceIdentifier: "writer-1", IsClusterWriter: true }]),
          instances: [instance("writer-1")],
        },
      ]),
    });

    expect(sequelize.connectionManager.pool).not.toBe(writePool);
    await balancer.destroy();
    expect(sequelize.connectionManager.pool).toBe(writePool);
  });

  it("restores the original v7 pool on destroy", async () => {
    const { sequelize, writePool } = createFakeV7Sequelize();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([{ DBInstanceIdentifier: "writer-1", IsClusterWriter: true }]),
          instances: [instance("writer-1")],
        },
      ]),
    });

    expect(sequelize.pool).not.toBe(writePool);
    await balancer.destroy();
    expect(sequelize.pool).toBe(writePool);
  });

  it("destroys writer fallback connections through the original write pool", async () => {
    const { sequelize, writePool } = createFakeV6Sequelize();
    const balancer = attachRdsReplicaBalancer(sequelize, {
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient: createRdsClientSequence([
        {
          cluster: cluster([{ DBInstanceIdentifier: "writer-1", IsClusterWriter: true }]),
          instances: [instance("writer-1")],
        },
      ]),
      fallbackToWriter: true,
    });

    await balancer.syncNow();
    const connection = await sequelize.connectionManager.pool.acquire("SELECT", false);
    await sequelize.connectionManager.pool.destroy(connection);

    expect(writePool.destroyed).toEqual([connection]);
    await balancer.destroy();
  });

  it("constructs, syncs, and starts through createRdsAwareSequelize", async () => {
    class FakeSequelizeCtor {
      connectionManager = createFakeV6Sequelize().sequelize.connectionManager;
      readonly options: Record<string, unknown>;

      constructor(options: unknown) {
        this.options = options as Record<string, unknown>;
      }
    }

    const { sequelize, balancer } = await createRdsAwareSequelize(
      FakeSequelizeCtor,
      { dialect: "postgres" },
      {
        clusterIdentifier: "app-cluster",
        region: "us-east-1",
        rdsClient: createRdsClientSequence([
          {
            cluster: cluster([
              { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
              { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
            ]),
            instances: [instance("writer-1"), instance("reader-1")],
          },
        ]),
        pollIntervalMs: 60_000,
      },
    );

    expect(sequelize.options).toMatchObject({ dialect: "postgres", host: "writer.cluster.local", port: 5432 });
    expect(balancer.getTopology()?.readers[0]?.instanceIdentifier).toBe("reader-1");

    await balancer.destroy();
  });
});

function hostOf(connection: unknown): unknown {
  return (connection as { host?: unknown }).host;
}
