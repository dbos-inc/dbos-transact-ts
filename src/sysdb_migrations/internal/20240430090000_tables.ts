/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240430090000_tables: ReadonlyArray<SqlStatement> = [
  {
    sql: `create table "dbos"."scheduler_state" ("workflow_fn_name" text not null, "last_run_time" bigint not null, constraint "scheduler_state_pkey" primary key ("workflow_fn_name"))`,
    bindings: [],
  },
];

const down_pg__20240430090000_tables: ReadonlyArray<SqlStatement> = [
  { sql: `drop table if exists "dbos"."scheduler_state"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240430090000_tables',
  up: {
    pg: up_pg__20240430090000_tables,
  },
  down: {
    pg: down_pg__20240430090000_tables,
  },
};
