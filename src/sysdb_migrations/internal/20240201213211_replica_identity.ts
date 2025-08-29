/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240201213211_replica_identity: ReadonlyArray<SqlStatement> = [
  { sql: `create extension if not exists "uuid-ossp"`, bindings: [] },
  { sql: `alter table "dbos"."notifications" add column "message_uuid" text default uuid_generate_v4()`, bindings: [] },
  {
    sql: `alter table "dbos"."notifications" add constraint "notifications_pkey" primary key ("message_uuid")`,
    bindings: [],
  },
];

const down_pg__20240201213211_replica_identity: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."notifications" drop column "message_uuid"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240201213211_replica_identity',
  up: {
    pg: up_pg__20240201213211_replica_identity,
  },
  down: {
    pg: down_pg__20240201213211_replica_identity,
  },
};
