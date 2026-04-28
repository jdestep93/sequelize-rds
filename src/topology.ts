import {
  DescribeDBClustersCommand,
  DescribeDBInstancesCommand,
  type DBCluster,
  type DBClusterMember,
  type DBInstance,
} from "@aws-sdk/client-rds";
import { RDSClient } from "@aws-sdk/client-rds";

import { RdsTopologyError } from "./errors.js";
import type {
  RdsClientLike,
  RdsClusterTopology,
  RdsReplicaBalancerOptions,
  RdsUnavailableReader,
} from "./types.js";

export interface TopologyDiscoveryOptions
  extends Pick<
    RdsReplicaBalancerOptions,
    | "clusterIdentifier"
    | "region"
    | "rdsClient"
    | "awsCredentials"
    | "awsClientConfig"
    | "includePromotionTiers"
    | "excludeInstanceIdentifiers"
  > {}

export function createRdsClient(
  options: Pick<RdsReplicaBalancerOptions, "region" | "rdsClient" | "awsCredentials" | "awsClientConfig">,
): RdsClientLike {
  return (
    options.rdsClient ??
    new RDSClient({
      ...options.awsClientConfig,
      region: options.region,
      credentials: options.awsCredentials,
    })
  );
}

export async function discoverRdsTopology(options: TopologyDiscoveryOptions): Promise<RdsClusterTopology> {
  const client = createRdsClient(options);
  const cluster = await describeCluster(client, options.clusterIdentifier);
  const instances = await describeClusterInstances(client, cluster);
  const instanceById = new Map(instances.map((instance) => [instance.DBInstanceIdentifier, instance]));
  const excluded = new Set(options.excludeInstanceIdentifiers ?? []);
  const tiers = options.includePromotionTiers ? new Set(options.includePromotionTiers) : undefined;
  const unavailableReaders: RdsUnavailableReader[] = [];
  const readers = [];

  for (const member of cluster.DBClusterMembers ?? []) {
    const instanceIdentifier = member.DBInstanceIdentifier;

    if (!instanceIdentifier) {
      continue;
    }

    if (member.IsClusterWriter) {
      unavailableReaders.push({
        instanceIdentifier,
        promotionTier: member.PromotionTier,
        status: instanceById.get(instanceIdentifier)?.DBInstanceStatus,
        reason: "writer",
      });
      continue;
    }

    if (excluded.has(instanceIdentifier)) {
      unavailableReaders.push({
        instanceIdentifier,
        promotionTier: member.PromotionTier,
        status: instanceById.get(instanceIdentifier)?.DBInstanceStatus,
        reason: "excluded",
      });
      continue;
    }

    if (tiers && !tiers.has(member.PromotionTier ?? -1)) {
      unavailableReaders.push({
        instanceIdentifier,
        promotionTier: member.PromotionTier,
        status: instanceById.get(instanceIdentifier)?.DBInstanceStatus,
        reason: "promotion-tier",
      });
      continue;
    }

    const instance = instanceById.get(instanceIdentifier);

    if (!instance) {
      unavailableReaders.push({
        instanceIdentifier,
        promotionTier: member.PromotionTier,
        reason: "missing-instance",
      });
      continue;
    }

    if (instance.DBInstanceStatus !== "available") {
      unavailableReaders.push({
        instanceIdentifier,
        promotionTier: member.PromotionTier,
        status: instance.DBInstanceStatus,
        reason: "unavailable",
      });
      continue;
    }

    if (!instance.Endpoint?.Address) {
      unavailableReaders.push({
        instanceIdentifier,
        promotionTier: member.PromotionTier,
        status: instance.DBInstanceStatus,
        reason: "missing-endpoint",
      });
      continue;
    }

    readers.push({
      instanceIdentifier,
      promotionTier: member.PromotionTier,
      status: instance.DBInstanceStatus,
      host: instance.Endpoint.Address,
      port: instance.Endpoint.Port ?? cluster.Port ?? 5432,
    });
  }

  return {
    clusterIdentifier: options.clusterIdentifier,
    timestamp: new Date(),
    writer: cluster.Endpoint ? { host: cluster.Endpoint, port: cluster.Port ?? 5432 } : undefined,
    readerEndpoint: cluster.ReaderEndpoint ? { host: cluster.ReaderEndpoint, port: cluster.Port ?? 5432 } : undefined,
    readers,
    unavailableReaders,
  };
}

async function describeCluster(client: RdsClientLike, clusterIdentifier: string): Promise<DBCluster> {
  const output = await client.send(new DescribeDBClustersCommand({ DBClusterIdentifier: clusterIdentifier }));
  const cluster = output.DBClusters?.[0];

  if (!cluster) {
    throw new RdsTopologyError(`RDS cluster "${clusterIdentifier}" was not found.`);
  }

  return cluster;
}

async function describeClusterInstances(client: RdsClientLike, cluster: DBCluster): Promise<DBInstance[]> {
  const instanceIdentifiers = [
    ...new Set((cluster.DBClusterMembers ?? []).map((member) => member.DBInstanceIdentifier).filter(Boolean)),
  ];

  return Promise.all(
    instanceIdentifiers.map(async (identifier) => {
      const output = await client.send(new DescribeDBInstancesCommand({ DBInstanceIdentifier: identifier }));
      return output.DBInstances?.[0];
    }),
  ).then((instances) => instances.filter((instance): instance is DBInstance => Boolean(instance)));
}

export function topologySignature(topology: RdsClusterTopology): string {
  return JSON.stringify({
    writer: topology.writer,
    readers: topology.readers.map((reader) => ({
      instanceIdentifier: reader.instanceIdentifier,
      host: reader.host,
      port: reader.port,
      promotionTier: reader.promotionTier,
    })),
  });
}

export function getWriterMember(cluster: DBCluster): DBClusterMember | undefined {
  return cluster.DBClusterMembers?.find((member) => member.IsClusterWriter);
}
