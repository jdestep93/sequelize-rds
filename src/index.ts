export { NoActiveReadersError, RdsReplicaBalancerError, RdsTopologyError, ReadConsistencyContextError } from "./errors.js";
export { RdsReplicaBalancerController } from "./balancer.js";
export { discoverRdsTopology } from "./topology.js";
export type {
  CreateRdsAwareSequelizeResult,
  RdsClientLike,
  RdsClusterTopology,
  RdsEndpoint,
  RdsReaderEndpoint,
  RdsReaderLagOptions,
  RdsReaderState,
  RdsReadConsistencyOptions,
  RdsReplicaBalancer,
  RdsReplicaBalancerLogger,
  RdsReplicaBalancerOptions,
  RdsUnavailableReader,
  SequelizeConstructor,
  SequelizeConstructorInput,
  SequelizeLike,
} from "./types.js";

import { RdsReplicaBalancerController } from "./balancer.js";
import type {
  CreateRdsAwareSequelizeResult,
  RdsReplicaBalancerOptions,
  SequelizeConstructor,
  SequelizeConstructorInput,
  SequelizeLike,
} from "./types.js";

export function attachRdsReplicaBalancer<TSequelize extends SequelizeLike>(
  sequelize: TSequelize,
  options: RdsReplicaBalancerOptions,
): RdsReplicaBalancerController {
  return new RdsReplicaBalancerController(sequelize, options);
}

export async function createRdsAwareSequelize<TSequelize extends SequelizeLike>(
  SequelizeCtor: SequelizeConstructor<TSequelize>,
  sequelizeOptionsOrArgs: SequelizeConstructorInput,
  options: RdsReplicaBalancerOptions,
): Promise<CreateRdsAwareSequelizeResult<TSequelize>> {
  const constructorArgs = Array.isArray(sequelizeOptionsOrArgs) ? sequelizeOptionsOrArgs : [sequelizeOptionsOrArgs];
  const sequelize = new SequelizeCtor(...constructorArgs);
  const balancer = attachRdsReplicaBalancer(sequelize, options);
  await balancer.syncNow();
  balancer.start();
  return { sequelize, balancer };
}
