/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20252101000000_workflow_queues_executor_id: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_queue" add column "executor_id" text`, bindings: [] },
];

const down_pg__20252101000000_workflow_queues_executor_id: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_queue" drop column "executor_id"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20252101000000_workflow_queues_executor_id',
  up: {
    pg: up_pg__20252101000000_workflow_queues_executor_id,
  },
  down: {
    pg: down_pg__20252101000000_workflow_queues_executor_id,
  },
};
