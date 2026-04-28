import { installSequelizePoolAdapter, type SequelizePoolAdapter } from "./adapters.js";
import { discoverRdsTopology, topologySignature } from "./topology.js";
import type {
  RdsClusterTopology,
  RdsReplicaBalancer,
  RdsReplicaBalancerOptions,
  SequelizeLike,
} from "./types.js";

const DEFAULT_POLL_INTERVAL_MS = 30_000;
const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

export class RdsReplicaBalancerController implements RdsReplicaBalancer {
  readonly #sequelize: SequelizeLike;
  readonly #options: RdsReplicaBalancerOptions;
  readonly #adapter: SequelizePoolAdapter;
  #topology?: RdsClusterTopology;
  #signature?: string;
  #interval?: ReturnType<typeof setInterval>;
  #syncInFlight?: Promise<RdsClusterTopology>;

  constructor(sequelize: SequelizeLike, options: RdsReplicaBalancerOptions) {
    this.#sequelize = sequelize;
    this.#options = normalizeOptions(options);
    this.#adapter = installSequelizePoolAdapter(sequelize, {
      clusterIdentifier: this.#options.clusterIdentifier,
      drainTimeoutMs: this.#options.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS,
      fallbackToWriter: this.#options.fallbackToWriter ?? true,
      keepExistingWriteHost: this.#options.keepExistingWriteHost ?? false,
      logger: this.#options.logger,
    });

    if (this.#options.autoStart) {
      this.start();
    }
  }

  start(): void {
    if (this.#interval) {
      return;
    }

    void this.#safeSync();
    this.#interval = setInterval(() => {
      void this.#safeSync();
    }, this.#options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS);
    this.#interval.unref?.();
  }

  stop(): void {
    if (!this.#interval) {
      return;
    }

    clearInterval(this.#interval);
    this.#interval = undefined;
  }

  async syncNow(): Promise<RdsClusterTopology> {
    if (this.#syncInFlight) {
      return this.#syncInFlight;
    }

    this.#syncInFlight = this.#sync().finally(() => {
      this.#syncInFlight = undefined;
    });

    return this.#syncInFlight;
  }

  getTopology(): RdsClusterTopology | undefined {
    return this.#topology;
  }

  async destroy(): Promise<void> {
    this.stop();
    await this.#adapter.destroy();
  }

  async #sync(): Promise<RdsClusterTopology> {
    const topology = await discoverRdsTopology(this.#options);
    const nextSignature = topologySignature(topology);
    this.#adapter.applyTopology(topology);
    this.#topology = topology;

    if (nextSignature !== this.#signature) {
      this.#signature = nextSignature;
      await this.#options.onTopologyChange?.(topology);
    }

    return topology;
  }

  async #safeSync(): Promise<void> {
    try {
      await this.syncNow();
    } catch (error) {
      this.#options.logger?.warn?.("RDS replica topology sync failed.", { error });
    }
  }
}

function normalizeOptions(options: RdsReplicaBalancerOptions): RdsReplicaBalancerOptions {
  return {
    ...options,
    pollIntervalMs: options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
    drainTimeoutMs: options.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS,
    fallbackToWriter: options.fallbackToWriter ?? true,
    keepExistingWriteHost: options.keepExistingWriteHost ?? false,
    autoStart: options.autoStart ?? false,
  };
}
