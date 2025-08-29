/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20252523000000_consolidate_inputs: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_status" add column "inputs" text null`, bindings: [] },
];

const down_pg__20252523000000_consolidate_inputs: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_status" drop column "inputs"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20252523000000_consolidate_inputs',
  up: {
    pg: up_pg__20252523000000_consolidate_inputs,
  },
  down: {
    pg: down_pg__20252523000000_consolidate_inputs,
  },
};
