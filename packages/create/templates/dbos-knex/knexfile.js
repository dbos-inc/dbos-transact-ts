const { parseConfigFile } = require('@dbos-inc/dbos-sdk');

const [dbosConfig] = parseConfigFile();

const config = {
  client: 'pg',
  connection: dbosConfig.poolConfig.connectionString,
  migrations: {
    directory: './migrations',
  },
};

module.exports = config;
