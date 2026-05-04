import { execFileSync } from "node:child_process";

import {
  DescribeDBClustersCommand,
  DescribeDBInstancesCommand,
  type DBCluster,
  type DBInstance,
} from "@aws-sdk/client-rds";
import { Client as PgClient } from "pg";
import mysql from "mysql2/promise";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { NoActiveReadersError, attachRdsReplicaBalancer, type RdsClientLike } from "../../src/index.js";

export type DialectName = "postgres" | "mysql";
export type SequelizeVersion = "v6" | "v7";

export interface DbEndpoint {
  id: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  marker: string;
}

export interface IntegrationContext {
  writer: DbEndpoint;
  reader1: DbEndpoint;
  reader2: DbEndpoint;
}

export interface SequelizeHarness {
  sequelize: any;
  queryTypes: { SELECT: unknown };
  close(): Promise<void>;
}

export interface IntegrationSuiteOptions {
  title: string;
  dialect: DialectName;
  version: SequelizeVersion;
  createSequelize(context: IntegrationContext): SequelizeHarness;
}

export function runSequelizeIntegrationSuite(options: IntegrationSuiteOptions): void {
  const dockerAvailable = isDockerAvailable();
  const suite = dockerAvailable ? describe : describe.skip;

  suite(options.title, () => {
    let stack: DatabaseStack;

    beforeAll(async () => {
      stack = await startDatabaseStack(options.dialect);
    });

    afterAll(async () => {
      await stack?.stop();
    });

    it("routes read queries to discovered readers and adds new readers after sync", async () => {
      const { sequelize, queryTypes, close } = options.createSequelize(stack.context);
      const rdsClient = createMutableRdsClient(stack.context);
      rdsClient.setReaders([stack.context.reader1]);
      const balancer = attachRdsReplicaBalancer(sequelize, {
        clusterIdentifier: "app-cluster",
        region: "us-east-1",
        rdsClient,
        fallbackToWriter: false,
        keepExistingWriteHost: true,
      });

      try {
        await balancer.syncNow();
        await expect(readMarker(sequelize, queryTypes)).resolves.toBe("reader-1");

        rdsClient.setReaders([stack.context.reader1, stack.context.reader2]);
        await balancer.syncNow();
        const markers = [await readMarker(sequelize, queryTypes), await readMarker(sequelize, queryTypes)];
        expect(markers).toContain("reader-2");
      } finally {
        await balancer.destroy();
        await close();
      }
    });

    it("removes unavailable readers from scheduling while allowing in-flight connections to release", async () => {
      const { sequelize, queryTypes, close } = options.createSequelize(stack.context);
      const rdsClient = createMutableRdsClient(stack.context);
      rdsClient.setReaders([stack.context.reader1]);
      const balancer = attachRdsReplicaBalancer(sequelize, {
        clusterIdentifier: "app-cluster",
        region: "us-east-1",
        rdsClient,
        fallbackToWriter: false,
        keepExistingWriteHost: true,
        drainTimeoutMs: 50,
      });

      try {
        await balancer.syncNow();
        const inFlight = await acquireReadConnection(options.version, sequelize);

        rdsClient.setReaders([stack.context.reader1, stack.context.reader2], { reader1: "deleting" });
        await balancer.syncNow();

        await expect(readMarker(sequelize, queryTypes)).resolves.toBe("reader-2");
        expect(() => releaseConnection(options.version, sequelize, inFlight)).not.toThrow();
      } finally {
        await balancer.destroy();
        await close();
      }
    });

    it("routes reads to the writer when fallback is enabled and no readers are active", async () => {
      const { sequelize, queryTypes, close } = options.createSequelize(stack.context);
      const rdsClient = createMutableRdsClient(stack.context);
      rdsClient.setReaders([]);
      const balancer = attachRdsReplicaBalancer(sequelize, {
        clusterIdentifier: "app-cluster",
        region: "us-east-1",
        rdsClient,
        fallbackToWriter: true,
        keepExistingWriteHost: true,
      });

      try {
        await balancer.syncNow();
        await expect(readMarker(sequelize, queryTypes)).resolves.toBe("writer");
      } finally {
        await balancer.destroy();
        await close();
      }
    });

    it("throws NoActiveReadersError when fallback is disabled and no readers are active", async () => {
      const { sequelize, queryTypes, close } = options.createSequelize(stack.context);
      const rdsClient = createMutableRdsClient(stack.context);
      rdsClient.setReaders([]);
      const balancer = attachRdsReplicaBalancer(sequelize, {
        clusterIdentifier: "app-cluster",
        region: "us-east-1",
        rdsClient,
        fallbackToWriter: false,
        keepExistingWriteHost: true,
      });

      try {
        await balancer.syncNow();
        await expect(readMarker(sequelize, queryTypes)).rejects.toBeInstanceOf(NoActiveReadersError);
      } finally {
        await balancer.destroy();
        await close();
      }
    });
  });
}

export function createMutableRdsClient(context: IntegrationContext): RdsClientLike & {
  setReaders(readers: DbEndpoint[], statuses?: Record<string, string>): void;
} {
  let readers: DbEndpoint[] = [];
  let statuses: Record<string, string> = {};

  return {
    setReaders(nextReaders, nextStatuses = {}) {
      readers = nextReaders;
      statuses = nextStatuses;
    },
    async send(command) {
      if (command instanceof DescribeDBClustersCommand) {
        return { DBClusters: [clusterFor(context.writer, readers)] };
      }

      if (command instanceof DescribeDBInstancesCommand) {
        const id = command.input.DBInstanceIdentifier;
        const endpoint = [context.writer, context.reader1, context.reader2].find((candidate) => candidate.id === id);
        return { DBInstances: endpoint ? [dbInstance(endpoint, statuses[endpoint.id] ?? "available")] : [] };
      }

      throw new Error("Unexpected RDS command");
    },
  };
}

async function readMarker(sequelize: any, queryTypes: { SELECT: unknown }): Promise<string> {
  const rows = await sequelize.query("SELECT marker FROM sequelize_rds_marker LIMIT 1", {
    type: queryTypes.SELECT,
  });
  return String(rows[0]?.marker);
}

async function acquireReadConnection(version: SequelizeVersion, sequelize: any): Promise<unknown> {
  if (version === "v6") {
    return sequelize.connectionManager.pool.acquire("SELECT", false);
  }

  return sequelize.pool.acquire({ type: "read" });
}

function releaseConnection(version: SequelizeVersion, sequelize: any, connection: unknown): void {
  if (version === "v6") {
    sequelize.connectionManager.pool.release(connection);
    return;
  }

  sequelize.pool.release(connection);
}

function clusterFor(writer: DbEndpoint, readers: DbEndpoint[]): DBCluster {
  return {
    DBClusterIdentifier: "app-cluster",
    Endpoint: writer.host,
    Port: writer.port,
    DBClusterMembers: [
      { DBInstanceIdentifier: writer.id, IsClusterWriter: true },
      ...readers.map((reader) => ({ DBInstanceIdentifier: reader.id, IsClusterWriter: false })),
    ],
  };
}

function dbInstance(endpoint: DbEndpoint, status: string): DBInstance {
  return {
    DBInstanceIdentifier: endpoint.id,
    DBInstanceStatus: status,
    Endpoint: {
      Address: endpoint.host,
      Port: endpoint.port,
    },
  };
}

interface DatabaseStack {
  context: IntegrationContext;
  stop(): Promise<void>;
}

async function startDatabaseStack(dialect: DialectName): Promise<DatabaseStack> {
  const [writer, reader1, reader2] = await Promise.all([
    startDatabase(dialect, "writer"),
    startDatabase(dialect, "reader-1"),
    startDatabase(dialect, "reader-2"),
  ]);

  return {
    context: {
      writer: writer.endpoint,
      reader1: reader1.endpoint,
      reader2: reader2.endpoint,
    },
    async stop() {
      await Promise.allSettled([writer.container.stop(), reader1.container.stop(), reader2.container.stop()]);
    },
  };
}

async function startDatabase(
  dialect: DialectName,
  marker: "writer" | "reader-1" | "reader-2",
): Promise<{ container: StartedTestContainer; endpoint: DbEndpoint }> {
  if (dialect === "postgres") {
    const container = await new GenericContainer("postgres:16-alpine")
      .withEnvironment({
        POSTGRES_DB: "app",
        POSTGRES_USER: "app",
        POSTGRES_PASSWORD: "secret",
      })
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forListeningPorts())
      .withStartupTimeout(120_000)
      .start();
    const endpoint = endpointFor(container, dialect, marker, 5432);
    await seedPostgres(endpoint);
    return { container, endpoint };
  }

  const container = await new GenericContainer("mysql:8.4")
    .withEnvironment({
      MYSQL_DATABASE: "app",
      MYSQL_USER: "app",
      MYSQL_PASSWORD: "secret",
      MYSQL_ROOT_PASSWORD: "root-secret",
    })
    .withExposedPorts(3306)
    .withWaitStrategy(Wait.forListeningPorts())
    .withStartupTimeout(120_000)
    .start();
  const endpoint = endpointFor(container, dialect, marker, 3306);
  await seedMysql(endpoint);
  return { container, endpoint };
}

function endpointFor(
  container: StartedTestContainer,
  dialect: DialectName,
  marker: "writer" | "reader-1" | "reader-2",
  port: number,
): DbEndpoint {
  return {
    id: marker,
    host: container.getHost(),
    port: container.getMappedPort(port),
    database: "app",
    username: "app",
    password: "secret",
    marker,
  };
}

async function seedPostgres(endpoint: DbEndpoint): Promise<void> {
  await retry(async () => {
    const client = new PgClient({
      host: endpoint.host,
      port: endpoint.port,
      database: endpoint.database,
      user: endpoint.username,
      password: endpoint.password,
    });
    await client.connect();
    try {
      await client.query("CREATE TABLE sequelize_rds_marker (marker text NOT NULL)");
      await client.query("INSERT INTO sequelize_rds_marker (marker) VALUES ($1)", [endpoint.marker]);
    } finally {
      await client.end();
    }
  });
}

async function seedMysql(endpoint: DbEndpoint): Promise<void> {
  await retry(async () => {
    const connection = await mysql.createConnection({
      host: endpoint.host,
      port: endpoint.port,
      database: endpoint.database,
      user: endpoint.username,
      password: endpoint.password,
    });
    try {
      await connection.execute("CREATE TABLE sequelize_rds_marker (marker varchar(32) NOT NULL)");
      await connection.execute("INSERT INTO sequelize_rds_marker (marker) VALUES (?)", [endpoint.marker]);
    } finally {
      await connection.end();
    }
  });
}

async function retry<T>(operation: () => Promise<T>): Promise<T> {
  let lastError: unknown;

  for (let attempt = 0; attempt < 30; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => {
        setTimeout(resolve, 1_000);
      });
    }
  }

  throw lastError;
}

function isDockerAvailable(): boolean {
  try {
    execFileSync("docker", ["info"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}
