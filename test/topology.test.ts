import { describe, expect, it } from "vitest";

import { DescribeDBClustersCommand, DescribeDBInstancesCommand } from "@aws-sdk/client-rds";

import { RdsTopologyError, discoverRdsTopology } from "../src/index.js";
import { createRdsClient } from "../src/topology.js";
import { cluster, createRdsClientSequence, instance } from "./helpers.js";

describe("discoverRdsTopology", () => {
  it("returns only available non-writer instances as active readers", async () => {
    const rdsClient = createRdsClientSequence([
      {
        cluster: cluster([
          { DBInstanceIdentifier: "writer-1", IsClusterWriter: true, PromotionTier: 0 },
          { DBInstanceIdentifier: "reader-1", IsClusterWriter: false, PromotionTier: 2 },
          { DBInstanceIdentifier: "reader-2", IsClusterWriter: false, PromotionTier: 3 },
          { DBInstanceIdentifier: "reader-3", IsClusterWriter: false, PromotionTier: 4 },
        ]),
        instances: [
          instance("writer-1"),
          instance("reader-1"),
          instance("reader-2", "deleting"),
          instance("reader-3", "available", ""),
        ],
      },
    ]);

    const topology = await discoverRdsTopology({
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient,
    });

    expect(topology.writer).toEqual({ host: "writer.cluster.local", port: 5432 });
    expect(topology.readers).toEqual([
      {
        instanceIdentifier: "reader-1",
        promotionTier: 2,
        status: "available",
        host: "reader-1.local",
        port: 5432,
      },
    ]);
    expect(topology.unavailableReaders).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ instanceIdentifier: "writer-1", reason: "writer" }),
        expect.objectContaining({ instanceIdentifier: "reader-2", reason: "unavailable" }),
        expect.objectContaining({ instanceIdentifier: "reader-3", reason: "missing-endpoint" }),
      ]),
    );
  });

  it("filters readers by promotion tier and excluded instance identifiers", async () => {
    const rdsClient = createRdsClientSequence([
      {
        cluster: cluster([
          { DBInstanceIdentifier: "reader-1", IsClusterWriter: false, PromotionTier: 1 },
          { DBInstanceIdentifier: "reader-2", IsClusterWriter: false, PromotionTier: 10 },
        ]),
        instances: [instance("reader-1"), instance("reader-2")],
      },
    ]);

    const topology = await discoverRdsTopology({
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient,
      includePromotionTiers: [10],
      excludeInstanceIdentifiers: ["reader-2"],
    });

    expect(topology.readers).toEqual([]);
    expect(topology.unavailableReaders).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ instanceIdentifier: "reader-1", reason: "promotion-tier" }),
        expect.objectContaining({ instanceIdentifier: "reader-2", reason: "excluded" }),
      ]),
    );
  });

  it("describes only the DB instances that are members of the target cluster", async () => {
    const calls: string[] = [];
    const rdsClient = {
      async send(command: unknown) {
        if (command instanceof DescribeDBClustersCommand) {
          calls.push("cluster");
          return {
            DBClusters: [
              cluster([
                { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
                { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
                { DBInstanceIdentifier: "reader-2", IsClusterWriter: false },
              ]),
            ],
          };
        }

        if (command instanceof DescribeDBInstancesCommand) {
          const identifier = command.input.DBInstanceIdentifier;
          calls.push(`instance:${identifier}`);
          return { DBInstances: [instance(String(identifier))] };
        }

        throw new Error("Unexpected command");
      },
    };

    const topology = await discoverRdsTopology({
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient,
    });

    expect(calls).toEqual(["cluster", "instance:writer-1", "instance:reader-1", "instance:reader-2"]);
    expect(topology.readers.map((reader) => reader.instanceIdentifier)).toEqual(["reader-1", "reader-2"]);
  });

  it("marks cluster members missing from DescribeDBInstances as unavailable", async () => {
    const rdsClient = createRdsClientSequence([
      {
        cluster: cluster([
          { DBInstanceIdentifier: "writer-1", IsClusterWriter: true },
          { DBInstanceIdentifier: "reader-1", IsClusterWriter: false },
        ]),
        instances: [instance("writer-1")],
      },
    ]);

    const topology = await discoverRdsTopology({
      clusterIdentifier: "app-cluster",
      region: "us-east-1",
      rdsClient,
    });

    expect(topology.readers).toEqual([]);
    expect(topology.unavailableReaders).toEqual(
      expect.arrayContaining([expect.objectContaining({ instanceIdentifier: "reader-1", reason: "missing-instance" })]),
    );
  });

  it("throws a typed topology error when the RDS cluster is not found", async () => {
    const rdsClient = {
      async send(command: unknown) {
        if (command instanceof DescribeDBClustersCommand) {
          return { DBClusters: [] };
        }

        throw new Error("DescribeDBInstances should not be called");
      },
    };

    await expect(
      discoverRdsTopology({
        clusterIdentifier: "missing-cluster",
        region: "us-east-1",
        rdsClient,
      }),
    ).rejects.toBeInstanceOf(RdsTopologyError);
  });

  it("creates an AWS RDS client with explicitly provided credentials and client config", async () => {
    const client = createRdsClient({
      region: "us-west-2",
      awsCredentials: {
        accessKeyId: "custom-access-key",
        secretAccessKey: "custom-secret-key",
        sessionToken: "custom-session-token",
      },
      awsClientConfig: {
        endpoint: "https://rds.us-west-2.amazonaws.com",
      },
    });
    const config = (client as any).config;

    await expect(config.region()).resolves.toBe("us-west-2");
    await expect(config.credentials()).resolves.toMatchObject({
      accessKeyId: "custom-access-key",
      secretAccessKey: "custom-secret-key",
      sessionToken: "custom-session-token",
    });
    expect(config.endpoint).toBeDefined();
  });

  it("uses a provided rdsClient instead of constructing one from credential options", () => {
    const rdsClient = createRdsClientSequence([
      {
        cluster: cluster([]),
        instances: [],
      },
    ]);

    expect(
      createRdsClient({
        region: "us-east-1",
        rdsClient,
        awsCredentials: {
          accessKeyId: "ignored",
          secretAccessKey: "ignored",
        },
      }),
    ).toBe(rdsClient);
  });
});
