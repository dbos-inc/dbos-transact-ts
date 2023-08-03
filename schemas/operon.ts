const operonSystemDbSchema = `
  CREATE TABLE IF NOT EXISTS operon__FunctionOutputs (
    workflow_id VARCHAR(64) NOT NULL,
    function_id INT NOT NULL,
    output TEXT,
    error TEXT,
    PRIMARY KEY (workflow_id, function_id)
  );

  CREATE TABLE IF NOT EXISTS operon__WorkflowStatus (
    workflow_id VARCHAR(64) PRIMARY KEY,
    workflow_name TEXT,
    status VARCHAR(64),
    output TEXT,
    error TEXT,
    last_update BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM now())::bigint
  );

  CREATE TABLE IF NOT EXISTS operon__Notifications (
    key VARCHAR(255) PRIMARY KEY,
    message TEXT NOT NULL
  );

  CREATE OR REPLACE FUNCTION operon__NotificationsFunction() RETURNS TRIGGER AS $$
    DECLARE
    BEGIN
        -- Publish a notification for all keys
        PERFORM pg_notify('operon__notificationschannel', NEW.key::text);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO
    $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'operon__notificationstrigger') THEN
          EXECUTE '
              CREATE TRIGGER operon__notificationstrigger
              AFTER INSERT ON operon__Notifications
              FOR EACH ROW EXECUTE FUNCTION operon__NotificationsFunction()';
        END IF;
    END
    $$;
`;

export default operonSystemDbSchema;
