/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240123183021_tables: ReadonlyArray<SqlStatement> = [
  { sql: `(EXTRACT(EPOCH FROM now())*1000)::bigint`, bindings: [] },
  {
    sql: `create table "dbos"."operation_outputs" ("workflow_uuid" text not null, "function_id" integer not null, "output" text, "error" text, constraint "operation_outputs_pkey" primary key ("workflow_uuid", "function_id"))`,
    bindings: [],
  },
  {
    sql: `create table "dbos"."workflow_inputs" ("workflow_uuid" text not null, "inputs" text not null, constraint "workflow_inputs_pkey" primary key ("workflow_uuid"))`,
    bindings: [],
  },
  {
    sql: `create table "dbos"."workflow_status" ("workflow_uuid" text, "status" text, "name" text, "authenticated_user" text, "assumed_role" text, "authenticated_roles" text, "request" text, "output" text, "error" text, "executor_id" text, constraint "workflow_status_pkey" primary key ("workflow_uuid"))`,
    bindings: [],
  },
  {
    sql: `create table "dbos"."notifications" ("destination_uuid" text not null, "topic" text, "message" text not null, "created_at_epoch_ms" bigint not null default '[object Promise]')`,
    bindings: [],
  },
  {
    sql: `create table "dbos"."workflow_events" ("workflow_uuid" text not null, "key" text not null, "value" text not null, constraint "workflow_events_pkey" primary key ("workflow_uuid", "key"))`,
    bindings: [],
  },
];

const down_pg__20240123183021_tables: ReadonlyArray<SqlStatement> = [
  { sql: `drop table if exists "dbos"."operation_outputs"`, bindings: [] },
  { sql: `drop table if exists "dbos"."workflow_inputs"`, bindings: [] },
  { sql: `drop table if exists "dbos"."workflow_status"`, bindings: [] },
  { sql: `drop table if exists "dbos"."notifications"`, bindings: [] },
  { sql: `drop table if exists "dbos"."workflow_events"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240123183021_tables',
  up: {
    pg: up_pg__20240123183021_tables,
  },
  down: {
    pg: down_pg__20240123183021_tables,
  },
};
