/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20250319190617_add_childid_opoutputs: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."operation_outputs" add column "child_workflow_id" text`, bindings: [] },
];

const down_pg__20250319190617_add_childid_opoutputs: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."operation_outputs" drop column "child_workflow_id"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20250319190617_add_childid_opoutputs',
  up: {
    pg: up_pg__20250319190617_add_childid_opoutputs,
  },
  down: {
    pg: down_pg__20250319190617_add_childid_opoutputs,
  },
};
