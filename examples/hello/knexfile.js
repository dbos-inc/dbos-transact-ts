const { parseConfigFile } = require('@dbos-inc/dbos-sdk/dist/src/dbos-runtime/config');
const { DBOSConfig } = require('@dbos-inc/dbos-sdk/dist/src/dbos-executor');

const [dbosConfig, ] = parseConfigFile();

const config = {
  client: 'pg',
  connection: {
    host: dbosConfig.poolConfig.host,
    user: dbosConfig.poolConfig.user,
    password: dbosConfig.poolConfig.password,
    database: dbosConfig.poolConfig.database,
    ssl: dbosConfig.poolConfig.ssl,
  },
  migrations: {
    directory: './migrations'
  }
};

module.exports = config;
