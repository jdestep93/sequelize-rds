import { QueryTypes, Sequelize } from "sequelize";

import { runSequelizeIntegrationSuite, type IntegrationContext } from "./helpers.js";

runSequelizeIntegrationSuite({
  title: "sequelize v6 mysql integration",
  dialect: "mysql",
  version: "v6",
  createSequelize(context: IntegrationContext) {
    const sequelize = new Sequelize(context.writer.database, context.writer.username, context.writer.password, {
      dialect: "mysql",
      logging: false,
      pool: { max: 2, min: 0, idle: 500, acquire: 5_000, evict: 250 },
      replication: {
        write: connectionConfig(context.writer),
        read: [connectionConfig(context.writer)],
      },
    });

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
    username: endpoint.username,
    password: endpoint.password,
    database: endpoint.database,
  };
}
