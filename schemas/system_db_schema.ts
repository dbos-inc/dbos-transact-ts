export interface workflow_status {
  workflow_uuid: string;
  status: string;
  output: string;
  error: string;
  updated_at_epoch_ms: number;
}

export interface notifications {
  destination_uuid: string;
  topic: string;
  message: string;
}

export interface updates {
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

export const systemDBSchema = `
  CREATE SCHEMA IF NOT EXISTS operon;

  CREATE TABLE IF NOT EXISTS operon.operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INT NOT NULL,
    output TEXT,
    error TEXT,
    PRIMARY KEY (workflow_uuid, function_id)
  );

  CREATE TABLE IF NOT EXISTS operon.workflow_status (
    workflow_uuid TEXT PRIMARY KEY,
    status TEXT,
    output TEXT,
    error TEXT
  );

  CREATE TABLE IF NOT EXISTS operon.notifications (
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint
  );

  CREATE TABLE IF NOT EXISTS operon.updates (
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
          WHERE  schemaname = 'operon'
          AND    tablename  = 'notifications'
          AND    indexname  = 'idx_workflow_topic'
      ) THEN
          CREATE INDEX idx_workflow_topic ON operon.notifications (destination_uuid, topic);
      END IF;
  END 
  $$;

  CREATE OR REPLACE FUNCTION operon.notifications_function() RETURNS TRIGGER AS $$
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
              AFTER INSERT ON operon.notifications
              FOR EACH ROW EXECUTE FUNCTION operon.notifications_function()';
        END IF;
    END
    $$;

    CREATE OR REPLACE FUNCTION operon.updates_function() RETURNS TRIGGER AS $$
    DECLARE
        payload text := NEW.workflow_uuid || '::' || NEW.key;
    BEGIN
        -- Publish a notification for all keys
        PERFORM pg_notify('operon_updates_channel', payload);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO
    $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'operon_updates_trigger') THEN
          EXECUTE '
              CREATE TRIGGER operon_updates_trigger
              AFTER INSERT ON operon.updates
              FOR EACH ROW EXECUTE FUNCTION operon.updates_function()';
        END IF;
    END
    $$;
`;