/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240123182943_schema: ReadonlyArray<SqlStatement> = [
  { sql: `CREATE SCHEMA IF NOT EXISTS dbos`, bindings: [] },
];

const down_pg__20240123182943_schema: ReadonlyArray<SqlStatement> = [
  { sql: `DROP SCHEMA IF EXISTS dbos`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240123182943_schema',
  up: {
    pg: up_pg__20240123182943_schema,
  },
  down: {
    pg: down_pg__20240123182943_schema,
  },
};
