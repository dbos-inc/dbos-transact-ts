import { Knex } from 'knex';
import { parseConfigFile } from '@dbos-inc/dbos-sdk';

const { databaseUrl } = parseConfigFile();

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
