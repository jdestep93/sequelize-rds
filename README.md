# sequelize-rds

Dynamic RDS/Aurora reader pool synchronization for Sequelize.

> Alpha software: `sequelize-rds` is published as `0.1.0-alpha.0` and should be validated in your staging environment before production use. Sequelize v7 support is experimental because Sequelize v7 is still alpha.

`sequelize-rds` keeps Sequelize read traffic aligned with the current members of an AWS RDS/Aurora cluster. It polls RDS cluster topology, routes reads to active reader instance endpoints, adds new readers without recreating Sequelize, and drains readers that AWS reports as removed or unavailable.

It does not create, delete, or scale RDS resources.

## Install

```sh
npm install sequelize-rds @aws-sdk/client-rds sequelize-pool
```

Use one Sequelize peer dependency:

```sh
npm install sequelize
```

or:

```sh
npm install @sequelize/core
```

## Test status

The package includes two test layers:

```sh
npm run test:unit
npm run test:integration
```

`test:unit` runs the fast mocked RDS and pool tests. `test:integration` uses Testcontainers and Docker to run real Sequelize v6 and Sequelize v7 alpha instances against Postgres and MySQL containers. Integration tests skip automatically when Docker is unavailable.

CI runs:

```sh
npm ci
npm run typecheck
npm run test:unit
npm run build
npm run test:integration
```

## npm CI/CD

GitHub Actions includes:

- `.github/workflows/ci.yml`: runs on pushes and pull requests to `main`.
- `.github/workflows/publish-npm.yml`: runs on published GitHub releases or manual `workflow_dispatch`.

The publish workflow runs typecheck, unit tests, build, and integration tests before publishing. It publishes with npm provenance enabled.

Publishing supports either:

- npm trusted publishing for this GitHub repository, using the workflow name `Publish npm`.
- a GitHub Actions secret named `NPM_TOKEN` containing a granular npm token with package publish permission and 2FA bypass if your npm account requires it.

The npm dist-tag is inferred from `package.json`:

- `*-alpha*` -> `alpha`
- `*-beta*` -> `beta`
- `*-rc*` -> `next`
- otherwise -> `latest`

Manual workflow runs can override the dist-tag with the `npm-tag` input.

## Basic usage

```ts
import { Sequelize } from "sequelize";
import { attachRdsReplicaBalancer } from "sequelize-rds";

const sequelize = new Sequelize(process.env.DATABASE_URL!, {
  dialect: "postgres",
  pool: { max: 20, min: 0 },
});

const balancer = attachRdsReplicaBalancer(sequelize, {
  clusterIdentifier: "app-cluster",
  region: "us-east-1",
  autoStart: true,
});

await balancer.syncNow();
```

If `autoStart` is not enabled, call `balancer.start()` after the first sync. `createRdsAwareSequelize()` performs an initial sync and starts monitoring automatically.

## How monitoring works

The balancer polls AWS RDS with `DescribeDBClusters` and `DescribeDBInstances`.

- Readers are active only when they are cluster members, are not the writer, have `DBInstanceStatus === "available"`, and have an instance endpoint address.
- New active readers are added to read routing.
- Removed or unavailable readers are immediately removed from scheduling.
- In-flight connections to removed readers are allowed to release, then the reader pool is drained.
- Writes continue through Sequelize's write pool.
- If no active readers exist, reads fall back to the writer by default. Set `fallbackToWriter: false` to throw `NoActiveReadersError` instead.

Default polling interval is `30_000` ms. Override it with `pollIntervalMs`.

## AWS credentials

By default, the package uses the standard AWS SDK credential chain. This supports `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`, and role credentials from Lambda, ECS, EKS, or EC2.

If your app uses custom environment variable names, pass credentials directly:

```ts
attachRdsReplicaBalancer(sequelize, {
  clusterIdentifier: "app-cluster",
  region: process.env.APP_AWS_REGION ?? "us-east-1",
  awsCredentials: {
    accessKeyId: process.env.APP_AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.APP_AWS_SECRET_ACCESS_KEY!,
    sessionToken: process.env.APP_AWS_SESSION_TOKEN,
  },
  autoStart: true,
});
```

For advanced AWS SDK options, pass `awsClientConfig`:

```ts
attachRdsReplicaBalancer(sequelize, {
  clusterIdentifier: "app-cluster",
  region: "us-east-1",
  awsClientConfig: {
    maxAttempts: 5,
  },
});
```

For full control, pass a preconfigured `rdsClient`. A provided `rdsClient` takes precedence over `awsCredentials` and `awsClientConfig`.

```ts
import { RDSClient } from "@aws-sdk/client-rds";

const rdsClient = new RDSClient({
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.CUSTOM_ACCESS_KEY_ID!,
    secretAccessKey: process.env.CUSTOM_SECRET_ACCESS_KEY!,
  },
});

attachRdsReplicaBalancer(sequelize, {
  clusterIdentifier: "app-cluster",
  region: "us-east-1",
  rdsClient,
});
```

Minimum IAM permissions can be scoped to the cluster and the DB instances that belong to it:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "rds:DescribeDBClusters",
      "Resource": "arn:aws:rds:us-east-1:123456789012:cluster:app-cluster"
    },
    {
      "Effect": "Allow",
      "Action": "rds:DescribeDBInstances",
      "Resource": [
        "arn:aws:rds:us-east-1:123456789012:db:app-cluster-writer-1",
        "arn:aws:rds:us-east-1:123456789012:db:app-cluster-reader-*"
      ]
    }
  ]
}
```

The reader instance resource can be an exact list, a naming-pattern ARN, or a tag-scoped policy that matches how your autoscaling process creates readers. If new readers can have arbitrary names, the IAM policy must already cover those future DB instance ARNs or the package won't be able to describe them when they appear in the cluster.

## Options

```ts
interface RdsReplicaBalancerOptions {
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
```

Defaults:

- `pollIntervalMs`: `30000`
- `drainTimeoutMs`: `30000`
- `fallbackToWriter`: `true`
- `keepExistingWriteHost`: `false`
- `autoStart`: `false`

`keepExistingWriteHost: false` updates Sequelize's writer host from the RDS cluster writer endpoint when topology sync runs. Set it to `true` if your app intentionally uses an existing writer host, RDS Proxy, or a custom endpoint.

## Controller API

```ts
const balancer = attachRdsReplicaBalancer(sequelize, options);

await balancer.syncNow();
balancer.start();
balancer.stop();
const topology = balancer.getTopology();
await balancer.destroy();
```

`destroy()` stops polling, restores the original Sequelize pool, and destroys plugin-managed reader pools.

## Helper constructor

```ts
import { createRdsAwareSequelize } from "sequelize-rds";

const { sequelize, balancer } = await createRdsAwareSequelize(
  Sequelize,
  [process.env.DATABASE_URL!, { dialect: "postgres", pool: { max: 20 } }],
  {
    clusterIdentifier: "app-cluster",
    region: "us-east-1",
  },
);
```

The helper constructs Sequelize, performs an initial `syncNow()`, starts polling, and returns both objects.
