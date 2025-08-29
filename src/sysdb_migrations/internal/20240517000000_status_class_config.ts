/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240517000000_status_class_config: ReadonlyArray<SqlStatement> = [
  {
    sql: `alter table "dbos"."workflow_status" add column "class_name" varchar(255) default null, add column "config_name" varchar(255) default null`,
    bindings: [],
  },
];

const down_pg__20240517000000_status_class_config: ReadonlyArray<SqlStatement> = [
  { sql: `alter table "dbos"."workflow_status" drop column "class_name"`, bindings: [] },
  { sql: `alter table "dbos"."workflow_status" drop column "config_name"`, bindings: [] },
];
export const migration: GeneratedMigration = {
  name: '20240517000000_status_class_config',
  up: {
    pg: up_pg__20240517000000_status_class_config,
  },
  down: {
    pg: down_pg__20240517000000_status_class_config,
  },
};
