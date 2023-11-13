// knexfile.ts

import { Knex } from 'knex';
import { buildConfigs } from '@dbos-inc/operon/dist/src/operon-runtime/config'
import { OperonConfig } from '@dbos-inc/operon/dist/src/operon';

const [operonConfig, ]: [OperonConfig, unknown] = buildConfigs();

const config: Knex.Config = {
  client: 'pg',
  connection: {
    host: operonConfig.poolConfig.host,
    user: operonConfig.poolConfig.user,
    password: operonConfig.poolConfig.password,
    database: operonConfig.poolConfig.database,
    ssl: operonConfig.poolConfig.ssl,
  },
  migrations: {
    directory: './migrations'
  }
};

export default config;
