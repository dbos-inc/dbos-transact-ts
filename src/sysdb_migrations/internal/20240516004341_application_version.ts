/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240516004341_application_version: ReadonlyArray<SqlStatement> = [
  {
    sql: `alter table "dbos"."workflow_status" add column "application_version" text, add column "application_id" text`,
    bindings: [],
  },
];

const down_pg__20240516004341_application_version: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_status" drop column "application_version"`, bindings: [] },
  { sql: `alter table "dbos"."workflow_status" drop column "application_id"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240516004341_application_version',
  up: {
    pg: up_pg__20240516004341_application_version,
  },
  down: {
    pg: down_pg__20240516004341_application_version,
  },
};
