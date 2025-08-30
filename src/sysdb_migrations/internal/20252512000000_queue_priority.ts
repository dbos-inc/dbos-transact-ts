/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20252512000000_queue_priority: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_queue" add column "priority" integer not null default '0'`, bindings: [] },
];

const down_pg__20252512000000_queue_priority: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_queue" drop column "priority"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20252512000000_queue_priority',
  up: {
    pg: up_pg__20252512000000_queue_priority,
  },
  down: {
    pg: down_pg__20252512000000_queue_priority,
  },
};
