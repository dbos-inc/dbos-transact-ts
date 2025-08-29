/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240205223925_foreign_keys: ReadonlyArray<SqlStatement> = [
  { sql: `create index "workflow_status_created_at_index" on "dbos"."workflow_status" ("created_at")`, bindings: [] },
  {
    sql: `alter table "dbos"."operation_outputs" add constraint "operation_outputs_workflow_uuid_foreign" foreign key ("workflow_uuid") references "dbos"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
    bindings: [],
  },
  {
    sql: `alter table "dbos"."workflow_inputs" add constraint "workflow_inputs_workflow_uuid_foreign" foreign key ("workflow_uuid") references "dbos"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
    bindings: [],
  },
  {
    sql: `alter table "dbos"."notifications" add constraint "notifications_destination_uuid_foreign" foreign key ("destination_uuid") references "dbos"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
    bindings: [],
  },
  {
    sql: `alter table "dbos"."workflow_events" add constraint "workflow_events_workflow_uuid_foreign" foreign key ("workflow_uuid") references "dbos"."workflow_status" ("workflow_uuid") on update CASCADE on delete CASCADE`,
    bindings: [],
  },
];

const down_pg__20240205223925_foreign_keys: ReadonlyArray<SqlStatement> = [
  { sql: `drop index "dbos"."workflow_status_created_at_index"`, bindings: [] },
  {
    sql: `alter table "dbos"."operation_outputs" drop constraint "operation_outputs_workflow_uuid_foreign"`,
    bindings: [],
  },
  { sql: `alter table "dbos"."workflow_inputs" drop constraint "workflow_inputs_workflow_uuid_foreign"`, bindings: [] },
  { sql: `alter table "dbos"."notifications" drop constraint "notifications_destination_uuid_foreign"`, bindings: [] },
  { sql: `alter table "dbos"."workflow_events" drop constraint "workflow_events_workflow_uuid_foreign"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240205223925_foreign_keys',
  up: {
    pg: up_pg__20240205223925_foreign_keys,
  },
  down: {
    pg: down_pg__20240205223925_foreign_keys,
  },
};
