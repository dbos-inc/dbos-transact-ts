export interface transaction_outputs {
  workflow_uuid: string;
  function_id: number;
  output: string;
  error: string;
  txn_id: string;
  txn_snapshot: string;
}

export const createUserDBSchema = `CREATE SCHEMA IF NOT EXISTS operon;`;

export const userDBSchema = `
  CREATE TABLE IF NOT EXISTS operon.transaction_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INT NOT NULL,
    output TEXT,
    error TEXT,
    txn_id TEXT,
    txn_snapshot TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id)
  );
`;
