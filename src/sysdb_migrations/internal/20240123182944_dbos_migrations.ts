/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240123182944_dbos_migrations: ReadonlyArray<SqlStatement> = [
  {
    sql: `create table "dbos"."dbos_migrations" ("version" bigint not null, constraint "dbos_migrations_pkey" primary key ("version"))`,
    bindings: [],
  },
];

const down_pg__20240123182944_dbos_migrations: ReadonlyArray<SqlStatement> = [
  { sql: `drop table "dbos"."dbos_migrations"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240123182944_dbos_migrations',
  up: {
    pg: up_pg__20240123182944_dbos_migrations,
  },
  down: {
    pg: down_pg__20240123182944_dbos_migrations,
  },
};
