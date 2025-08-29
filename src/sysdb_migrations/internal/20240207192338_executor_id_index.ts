/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240207192338_executor_id_index: ReadonlyArray<SqlStatement> = [
  { sql: `create index "workflow_status_executor_id_index" on "dbos"."workflow_status" ("executor_id")`, bindings: [] },
];

const down_pg__20240207192338_executor_id_index: ReadonlyArray<SqlStatement> = [
  { sql: `drop index "dbos"."workflow_status_executor_id_index"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240207192338_executor_id_index',
  up: {
    pg: up_pg__20240207192338_executor_id_index,
  },
  down: {
    pg: down_pg__20240207192338_executor_id_index,
  },
};
