exports.up = function (knex) {
  return knex.raw(`  
    CREATE OR REPLACE FUNCTION dbos.workflow_cancel_function() RETURNS TRIGGER AS $$
    DECLARE
        payload json;
    BEGIN
        IF TG_OP = 'INSERT' THEN
            payload = json_build_object(
                'wfid', NEW.workflow_id,
                'cancelled', 't'
            );
        ELSIF TG_OP = 'DELETE' THEN
            payload = json_build_object(
                'wfid', OLD.workflow_id,
                'cancelled', 'f'
            );
        END IF;

        PERFORM pg_notify('dbos_workflow_cancel_channel', payload::text);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER dbos_workflow_cancel_trigger
    AFTER INSERT OR UPDATE OR DELETE ON dbos.workflow_cancel
    FOR EACH ROW EXECUTE FUNCTION dbos.workflow_cancel_function();
  `);
};

exports.down = function (knex) {
  return knex.raw(`
    DROP TRIGGER IF EXISTS dbos_workflow_cancel_trigger ON dbos.workflow_cancel;
    DROP FUNCTION IF EXISTS dbos.workflow_cancel_function;
  `);
};
