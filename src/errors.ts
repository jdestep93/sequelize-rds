export class RdsReplicaBalancerError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "RdsReplicaBalancerError";
  }
}

export class NoActiveReadersError extends RdsReplicaBalancerError {
  constructor(clusterIdentifier: string) {
    super(`No active RDS reader instances are available for cluster "${clusterIdentifier}".`);
    this.name = "NoActiveReadersError";
  }
}

export class RdsTopologyError extends RdsReplicaBalancerError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "RdsTopologyError";
  }
}
