/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20250312171547_function_name_op_outputs: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."operation_outputs" add column "function_name" text not null default ''`, bindings: [] },
];

const down_pg__20250312171547_function_name_op_outputs: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."operation_outputs" drop column "function_name"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20250312171547_function_name_op_outputs',
  up: {
    pg: up_pg__20250312171547_function_name_op_outputs,
  },
  down: {
    pg: down_pg__20250312171547_function_name_op_outputs,
  },
};
