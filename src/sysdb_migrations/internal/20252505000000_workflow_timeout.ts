/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20252505000000_workflow_timeout: ReadonlyArray<SqlStatement> = [
  {
    sql: `alter table "dbos"."workflow_status" add column "workflow_timeout_ms" bigint, add column "workflow_deadline_epoch_ms" bigint`,
    bindings: [],
  },
];

const down_pg__20252505000000_workflow_timeout: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_status" drop column "workflow_timeout_ms"`, bindings: [] },
  { sql: `alter table "dbos"."workflow_status" drop column "workflow_deadline_epoch_ms"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20252505000000_workflow_timeout',
  up: {
    pg: up_pg__20252505000000_workflow_timeout,
  },
  down: {
    pg: down_pg__20252505000000_workflow_timeout,
  },
};
