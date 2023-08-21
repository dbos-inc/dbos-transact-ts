export interface transaction_outputs {
  workflow_uuid: string;
  function_id: number;
  output: string;
  error: string;
  transaction_id: string;
}

export const createUserDBSchema = `CREATE SCHEMA IF NOT EXISTS operon;`;

export const userDBSchema = `
  CREATE TABLE IF NOT EXISTS operon.transaction_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INT NOT NULL,
    output TEXT,
    error TEXT,
    transaction_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id)
  );
`;
