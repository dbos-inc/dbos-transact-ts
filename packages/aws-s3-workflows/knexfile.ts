import { Knex } from 'knex';
import { getConfiguredDatabaseUrl } from '@dbos-inc/dbos-sdk';

const { databaseUrl } = getConfiguredDatabaseUrl();

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
