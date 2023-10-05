export interface workflow_status {
  workflow_uuid: string;
  status: string;
  name: string;
  authenticated_user: string;
  output: string;
  error: string;
  assumed_role: string;
  authenticated_roles: string;  // Serialized list of roles.
}

export interface notifications {
  destination_uuid: string;
  topic: string;
  message: string;
}

export interface workflow_events {
  workflow_uuid: string;
  key: string;
  value: string;
}

export interface operation_outputs {
  workflow_uuid: string;
  function_id: number;
  output: string;
  error: string;
}

export interface workflow_inputs {
  workflow_uuid: string;
  inputs: string;
}

export const systemDBSchema = `
  CREATE TABLE IF NOT EXISTS operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INT NOT NULL,
    output TEXT,
    error TEXT,
    PRIMARY KEY (workflow_uuid, function_id)
  );

  CREATE TABLE IF NOT EXISTS workflow_inputs (
    workflow_uuid TEXT PRIMARY KEY NOT NULL,
    inputs TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS workflow_status (
    workflow_uuid TEXT PRIMARY KEY,
    status TEXT,
    name TEXT,
    authenticated_user TEXT,
    assumed_role TEXT,
    authenticated_roles TEXT,
    output TEXT,
    error TEXT
  );

  CREATE TABLE IF NOT EXISTS notifications (
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint
  );

  CREATE TABLE IF NOT EXISTS workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, key)
  );

  DO $$ 
  BEGIN 
      IF NOT EXISTS (
          SELECT 1
          FROM   pg_indexes 
          WHERE  tablename  = 'notifications'
          AND    indexname  = 'idx_workflow_topic'
      ) THEN
          CREATE INDEX idx_workflow_topic ON notifications (destination_uuid, topic);
      END IF;
  END 
  $$;

  CREATE OR REPLACE FUNCTION notifications_function() RETURNS TRIGGER AS $$
    DECLARE
        payload text := NEW.destination_uuid || '::' || NEW.topic;
    BEGIN
        -- Publish a notification for all keys
        PERFORM pg_notify('operon_notifications_channel', payload);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO
    $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'operon_notifications_trigger') THEN
          EXECUTE '
              CREATE TRIGGER operon_notifications_trigger
              AFTER INSERT ON notifications
              FOR EACH ROW EXECUTE FUNCTION notifications_function()';
        END IF;
    END
    $$;

    CREATE OR REPLACE FUNCTION workflow_events_function() RETURNS TRIGGER AS $$
    DECLARE
        payload text := NEW.workflow_uuid || '::' || NEW.key;
    BEGIN
        -- Publish a notification for all keys
        PERFORM pg_notify('operon_workflow_events_channel', payload);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO
    $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'operon_workflow_events_trigger') THEN
          EXECUTE '
              CREATE TRIGGER operon_workflow_events_trigger
              AFTER INSERT ON workflow_events
              FOR EACH ROW EXECUTE FUNCTION workflow_events_function()';
        END IF;
    END
    $$;
`;