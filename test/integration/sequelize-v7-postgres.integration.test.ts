import { QueryTypes, Sequelize } from "@sequelize/core";
import { PostgresDialect } from "@sequelize/postgres";

import { runSequelizeIntegrationSuite, type IntegrationContext } from "./helpers.js";

runSequelizeIntegrationSuite({
  title: "sequelize v7 postgres integration",
  dialect: "postgres",
  version: "v7",
  createSequelize(context: IntegrationContext) {
    const sequelize = new Sequelize({
      dialect: PostgresDialect,
      logging: false,
      pool: { max: 2, min: 0, idle: 500, acquire: 5_000, evict: 250 },
      ...connectionConfig(context.writer),
      replication: {
        write: connectionConfig(context.writer),
        read: [connectionConfig(context.writer)],
      },
    } as any);

    return {
      sequelize,
      queryTypes: QueryTypes,
      close: () => sequelize.close(),
    };
  },
});

function connectionConfig(endpoint: IntegrationContext["writer"]) {
  return {
    host: endpoint.host,
    port: endpoint.port,
    user: endpoint.username,
    password: endpoint.password,
    database: endpoint.database,
  };
}
