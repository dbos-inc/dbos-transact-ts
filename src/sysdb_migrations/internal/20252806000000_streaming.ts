/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20252806000000_streaming: ReadonlyArray<SqlStatement> = [
  {
    sql: `create table "dbos"."streams" ("workflow_uuid" text not null, "key" text not null, "value" text not null, "offset" integer not null, constraint "streams_pkey" primary key ("workflow_uuid", "key", "offset"))`,
    bindings: [],
  },
  {
    sql: `alter table "dbos"."streams" add constraint "streams_workflow_uuid_foreign" foreign key ("workflow_uuid") references "dbos"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
    bindings: [],
  },
];

const down_pg__20252806000000_streaming: ReadonlyArray<SqlStatement> = [
  { sql: `drop table "dbos"."streams"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20252806000000_streaming',
  up: {
    pg: up_pg__20252806000000_streaming,
  },
  down: {
    pg: down_pg__20252806000000_streaming,
  },
};
