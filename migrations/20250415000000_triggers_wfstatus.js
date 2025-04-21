exports.up = function (knex) {
  return knex.raw(`  
    CREATE OR REPLACE FUNCTION dbos.workflow_status_function() RETURNS TRIGGER AS $$
    DECLARE
        payload json;
    BEGIN
        IF TG_OP = 'INSERT' THEN
            payload = json_build_object(
                'wfid', NEW.workflow_uuid,
                'status', NEW.status,
                'oldstatus', ''
            );
        ELSIF TG_OP = 'UPDATE' THEN
            payload = json_build_object(
                'wfid', NEW.workflow_uuid,
                'status', NEW.status,
                'oldstatus', OLD.status
            );
        ELSIF TG_OP = 'DELETE' THEN
            payload = json_build_object(
                'wfid', NEW.workflow_uuid,
                'status', '',
                'oldstatus', OLD.status
            );
        END IF;

        PERFORM pg_notify('dbos_workflow_status_channel', payload::text);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER dbos_workflow_status_trigger
    AFTER INSERT OR UPDATE OR DELETE ON dbos.workflow_status
    FOR EACH ROW EXECUTE FUNCTION dbos.workflow_status_function();
  `);
};

exports.down = function (knex) {
  return knex.raw(`
    DROP TRIGGER IF EXISTS dbos_workflow_status_trigger ON dbos.workflow_status;
    DROP FUNCTION IF EXISTS dbos.workflow_status_function;
  `);
};
