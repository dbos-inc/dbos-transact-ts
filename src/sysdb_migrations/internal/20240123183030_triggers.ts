/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from '../migration_types';
const up_pg__20240123183030_triggers: ReadonlyArray<SqlStatement> = [
  {
    sql: `
      CREATE OR REPLACE FUNCTION dbos.notifications_function() RETURNS TRIGGER AS $$
      DECLARE
          payload text := NEW.destination_uuid || '::' || NEW.topic;
      BEGIN
          PERFORM pg_notify('dbos_notifications_channel', payload);
          RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
  
      CREATE TRIGGER dbos_notifications_trigger
      AFTER INSERT ON dbos.notifications
      FOR EACH ROW EXECUTE FUNCTION dbos.notifications_function();
  
      CREATE OR REPLACE FUNCTION dbos.workflow_events_function() RETURNS TRIGGER AS $$
      DECLARE
          payload text := NEW.workflow_uuid || '::' || NEW.key;
      BEGIN
          PERFORM pg_notify('dbos_workflow_events_channel', payload);
          RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
  
      CREATE TRIGGER dbos_workflow_events_trigger
      AFTER INSERT ON dbos.workflow_events
      FOR EACH ROW EXECUTE FUNCTION dbos.workflow_events_function();
    `,
    bindings: [],
  },
];

const down_pg__20240123183030_triggers: ReadonlyArray<SqlStatement> = [
  {
    sql: `
      DROP TRIGGER IF EXISTS dbos_notifications_trigger ON dbos.notifications;
      DROP FUNCTION IF EXISTS dbos.notifications_function;
      DROP TRIGGER IF EXISTS dbos_workflow_events_trigger ON dbos.workflow_events;
      DROP FUNCTION IF EXISTS dbos.workflow_events_function;
    `,
    bindings: [],
  },
];
export const migration: GeneratedMigration = {
  name: '20240123183030_triggers',
  up: {
    pg: up_pg__20240123183030_triggers,
  },
  down: {
    pg: down_pg__20240123183030_triggers,
  },
};
