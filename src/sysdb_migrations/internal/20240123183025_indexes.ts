/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240123183025_indexes: ReadonlyArray<SqlStatement> = [
  { sql: `create index "idx_workflow_topic" on "dbos"."notifications" ("destination_uuid", "topic")`, bindings: [] },
];

const down_pg__20240123183025_indexes: ReadonlyArray<SqlStatement> = [
  { sql: `drop index "dbos"."idx_workflow_topic"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240123183025_indexes',
  up: {
    pg: up_pg__20240123183025_indexes,
  },
  down: {
    pg: down_pg__20240123183025_indexes,
  },
};
