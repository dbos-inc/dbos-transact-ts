/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20252501190959_queue_dedup_id: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_queue" add column "deduplication_id" text`, bindings: [] },
  {
    sql: `alter table "dbos"."workflow_queue" add constraint "workflow_queue_queue_name_deduplication_id_unique" unique ("queue_name", "deduplication_id")`,
    bindings: [],
  },
];

const down_pg__20252501190959_queue_dedup_id: ReadonlyArray<SqlStatement> = [
  {
    sql: `alter table "dbos"."workflow_queue" drop constraint "workflow_queue_queue_name_deduplication_id_unique"`,
    bindings: [],
  },
  { sql: `alter table "dbos"."workflow_queue" drop column "deduplication_id"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20252501190959_queue_dedup_id',
  up: {
    pg: up_pg__20252501190959_queue_dedup_id,
  },
  down: {
    pg: down_pg__20252501190959_queue_dedup_id,
  },
};
