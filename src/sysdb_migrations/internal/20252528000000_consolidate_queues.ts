/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20252528000000_consolidate_queues: ReadonlyArray<SqlStatement> = [
  {
    sql: `alter table "dbos"."workflow_status" add column "started_at_epoch_ms" bigint null, add column "deduplication_id" text null, add column "priority" integer not null default '0'`,
    bindings: [],
  },
  {
    sql: `alter table "dbos"."workflow_status" add constraint "uq_workflow_status_queue_name_dedup_id" unique ("queue_name", "deduplication_id")`,
    bindings: [],
  },
  { sql: `create index "workflow_status_status_index" on "dbos"."workflow_status" ("status")`, bindings: [] },
];

const down_pg__20252528000000_consolidate_queues: ReadonlyArray<SqlStatement> = [
  { sql: `drop index "dbos"."workflow_status_status_index"`, bindings: [] },
  {
    sql: `alter table "dbos"."workflow_status" drop constraint "uq_workflow_status_queue_name_dedup_id"`,
    bindings: [],
  },
  { sql: `alter table "dbos"."workflow_status" drop column "priority"`, bindings: [] },
  { sql: `alter table "dbos"."workflow_status" drop column "deduplication_id"`, bindings: [] },
  { sql: `alter table "dbos"."workflow_status" drop column "started_at_epoch_ms"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20252528000000_consolidate_queues',
  up: {
    pg: up_pg__20252528000000_consolidate_queues,
  },
  down: {
    pg: down_pg__20252528000000_consolidate_queues,
  },
};
