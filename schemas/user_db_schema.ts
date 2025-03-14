export interface transaction_outputs {
  workflow_uuid: string;
  function_id: number;
  output: string | null;
  error: string | null;
  txn_id: string | null;
  txn_snapshot: string;
}

export const createUserDBSchema = `CREATE SCHEMA IF NOT EXISTS dbos;`;

export const userDBSchema = `
  CREATE TABLE IF NOT EXISTS dbos.transaction_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INT NOT NULL,
    output TEXT,
    error TEXT,
    txn_id TEXT,
    txn_snapshot TEXT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    PRIMARY KEY (workflow_uuid, function_id)
  );
`;

export const userDBIndex = `
  CREATE INDEX IF NOT EXISTS transaction_outputs_created_at_index ON dbos.transaction_outputs (created_at);
`;
