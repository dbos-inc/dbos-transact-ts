const { parseConfigFile } = require('@dbos-inc/dbos-sdk');

const [dbosConfig, ] = parseConfigFile();

const config = {
  client: 'pg',
  connection: {
    host: dbosConfig.poolConfig.host,
    port: dbosConfig.poolConfig.port,
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
