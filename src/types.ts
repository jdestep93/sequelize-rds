import type { DescribeDBClustersCommand, DescribeDBInstancesCommand, RDSClientConfig } from "@aws-sdk/client-rds";

export type RdsCommand = DescribeDBClustersCommand | DescribeDBInstancesCommand;

export interface RdsClientLike {
  send(command: RdsCommand): Promise<any>;
}

export interface RdsReplicaBalancerLogger {
  debug?(message: string, metadata?: Record<string, unknown>): void;
  info?(message: string, metadata?: Record<string, unknown>): void;
  warn?(message: string, metadata?: Record<string, unknown>): void;
  error?(message: string, metadata?: Record<string, unknown>): void;
}

export interface RdsReplicaBalancerOptions {
  clusterIdentifier: string;
  region: string;
  rdsClient?: RdsClientLike;
  awsCredentials?: RDSClientConfig["credentials"];
  awsClientConfig?: Omit<RDSClientConfig, "credentials" | "region">;
  pollIntervalMs?: number;
  drainTimeoutMs?: number;
  fallbackToWriter?: boolean;
  includePromotionTiers?: number[];
  excludeInstanceIdentifiers?: string[];
  keepExistingWriteHost?: boolean;
  autoStart?: boolean;
  logger?: RdsReplicaBalancerLogger;
  onTopologyChange?: (topology: RdsClusterTopology) => void | Promise<void>;
}

export interface RdsEndpoint {
  host: string;
  port: number;
}

export interface RdsReaderEndpoint extends RdsEndpoint {
  instanceIdentifier: string;
  promotionTier?: number;
  status: string;
}

export interface RdsUnavailableReader {
  instanceIdentifier: string;
  promotionTier?: number;
  status?: string;
  reason: "excluded" | "promotion-tier" | "writer" | "unavailable" | "missing-endpoint" | "missing-instance";
}

export interface RdsClusterTopology {
  clusterIdentifier: string;
  timestamp: Date;
  writer?: RdsEndpoint;
  readerEndpoint?: RdsEndpoint;
  readers: RdsReaderEndpoint[];
  unavailableReaders: RdsUnavailableReader[];
}

export interface RdsReplicaBalancer {
  start(): void;
  stop(): void;
  syncNow(): Promise<RdsClusterTopology>;
  getTopology(): RdsClusterTopology | undefined;
  destroy(): Promise<void>;
}

export interface SequelizeLike {
  connectionManager?: {
    pool?: unknown;
    config?: Record<string, unknown>;
  };
  pool?: unknown;
  options?: Record<string, unknown>;
  config?: Record<string, unknown>;
}

export interface CreateRdsAwareSequelizeResult<TSequelize> {
  sequelize: TSequelize;
  balancer: RdsReplicaBalancer;
}

export type SequelizeConstructor<TSequelize> = new (...args: unknown[]) => TSequelize;

export type SequelizeConstructorInput = unknown[] | Record<string, unknown> | string;
