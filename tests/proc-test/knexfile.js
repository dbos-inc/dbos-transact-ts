const { readConfigFile, getDatabaseUrl } = require('@dbos-inc/dbos-sdk');

const dbosConfig = readConfigFile(__dirname);
const databaseUrl = getDatabaseUrl(dbosConfig);

const config = {
  client: 'pg',
  connection: databaseUrl,
  migrations: {
    directory: './migrations',
  },
};

module.exports = config;
