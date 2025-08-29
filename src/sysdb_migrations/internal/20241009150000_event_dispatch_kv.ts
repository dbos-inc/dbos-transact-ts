/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20241009150000_event_dispatch_kv: ReadonlyArray<SqlStatement> = [
  {
    sql: `create table "dbos"."event_dispatch_kv" ("service_name" text not null, "workflow_fn_name" text not null, "key" text not null, "value" text, "update_seq" decimal(38, 0), "update_time" decimal(38, 15), constraint "event_dispatch_kv_pkey" primary key ("service_name", "workflow_fn_name", "key"))`,
    bindings: [],
  },
];

const down_pg__20241009150000_event_dispatch_kv: ReadonlyArray<SqlStatement> = [
  { sql: `drop table if exists "dbos"."event_dispatch_kv"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20241009150000_event_dispatch_kv',
  up: {
    pg: up_pg__20241009150000_event_dispatch_kv,
  },
  down: {
    pg: down_pg__20241009150000_event_dispatch_kv,
  },
};
