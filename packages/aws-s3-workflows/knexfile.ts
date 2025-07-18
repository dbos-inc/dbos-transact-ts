import { Knex } from 'knex';
import { readConfigFile, getDatabaseUrl } from '@dbos-inc/dbos-sdk';

const dbosConfig = readConfigFile(__dirname);
const databaseUrl = getDatabaseUrl(dbosConfig);

const config: Knex.Config = {
  client: 'pg',
  connection: {
    connectionString: databaseUrl,
  },
  migrations: {
    directory: './migrations',
  },
};

export default config;
