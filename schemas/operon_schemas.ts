const operonSystemDbSchema = `
  CREATE SCHEMA IF NOT EXISTS operon;

  CREATE TABLE IF NOT EXISTS operon.function_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INT NOT NULL,
    output TEXT,
    error TEXT,
    PRIMARY KEY (workflow_uuid, function_id)
  );

  CREATE TABLE IF NOT EXISTS operon.workflow_status (
    workflow_uuid TEXT PRIMARY KEY,
    workflow_name TEXT,
    status TEXT,
    output TEXT,
    error TEXT,
    updated_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint
  );

  CREATE TABLE IF NOT EXISTS operon.notifications (
    topic TEXT NOT NULL,
    key TEXT NOT NULL,
    message TEXT NOT NULL,
    PRIMARY KEY (topic, key)
  );

  CREATE OR REPLACE FUNCTION operon.notifications_function() RETURNS TRIGGER AS $$
    DECLARE
        topic_key text := NEW.topic || '::' || NEW.key;
    BEGIN
        -- Publish a notification for all keys
        PERFORM pg_notify('operon_notifications_channel', topic_key);
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
`;

export default operonSystemDbSchema;
