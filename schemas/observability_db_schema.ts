export const observabilityDBSchema = `
  CREATE TABLE IF NOT EXISTS operations (
    workflow_name TEXT NOT NULL,
    function_name TEXT NOT NULL,
    authorized_roles TEXT[] default '{}',
    created_at_epoch_us BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()))::bigint,
    params TEXT[] default '{}',
    param_types TEXT[] default '{}'
  );
`;
