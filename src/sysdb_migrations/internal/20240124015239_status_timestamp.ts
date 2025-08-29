/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240124015239_status_timestamp: ReadonlyArray<SqlStatement> = [
  { sql: `(EXTRACT(EPOCH FROM now())*1000)::bigint`, bindings: [] },
  { sql: `(EXTRACT(EPOCH FROM now())*1000)::bigint`, bindings: [] },
  {
    sql: `alter table "dbos"."workflow_status" add column "created_at" bigint not null default '[object Promise]', add column "updated_at" bigint not null default '[object Promise]'`,
    bindings: [],
  },
];

const down_pg__20240124015239_status_timestamp: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_status" drop column "created_at"`, bindings: [] },
  { sql: `alter table "dbos"."workflow_status" drop column "updated_at"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240124015239_status_timestamp',
  up: {
    pg: up_pg__20240124015239_status_timestamp,
  },
  down: {
    pg: down_pg__20240124015239_status_timestamp,
  },
};
