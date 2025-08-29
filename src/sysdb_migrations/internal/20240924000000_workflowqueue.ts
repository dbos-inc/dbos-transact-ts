/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240924000000_workflowqueue: ReadonlyArray<SqlStatement> = [
  { sql: `(EXTRACT(EPOCH FROM now())*1000)::bigint`, bindings: [] },
  { sql: `alter table "dbos"."workflow_status" add column "queue_name" text default null`, bindings: [] },
  {
    sql: `create table "dbos"."workflow_queue" ("queue_name" text not null, "workflow_uuid" text not null, "created_at_epoch_ms" bigint not null default '[object Promise]', "started_at_epoch_ms" bigint, "completed_at_epoch_ms" bigint, constraint "workflow_queue_pkey" primary key ("workflow_uuid"))`,
    bindings: [],
  },
  {
    sql: `alter table "dbos"."workflow_queue" add constraint "workflow_queue_workflow_uuid_foreign" foreign key ("workflow_uuid") references "dbos"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
    bindings: [],
  },
];

const down_pg__20240924000000_workflowqueue: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_status" drop column "queue_name"`, bindings: [] },
  { sql: `drop table if exists "dbos"."workflow_queue"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240924000000_workflowqueue',
  up: {
    pg: up_pg__20240924000000_workflowqueue,
  },
  down: {
    pg: down_pg__20240924000000_workflowqueue,
  },
};
