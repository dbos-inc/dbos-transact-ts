import { Knex } from 'knex';
import { getDatabaseUrl } from '@dbos-inc/dbos-sdk';

const databaseUrl = getDatabaseUrl();

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
