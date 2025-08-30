/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240621000000_workflow_tries: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_status" add column "recovery_attempts" bigint default '0'`, bindings: [] },
];

const down_pg__20240621000000_workflow_tries: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_status" drop column "recovery_attempts"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240621000000_workflow_tries',
  up: {
    pg: up_pg__20240621000000_workflow_tries,
  },
  down: {
    pg: down_pg__20240621000000_workflow_tries,
  },
};
