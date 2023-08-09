export interface transaction_outputs {
  workflow_uuid: string;
  function_id: number;
  output: string;
  error: string;
}

export const userDBSchema = `
  CREATE SCHEMA IF NOT EXISTS operon;

  CREATE TABLE IF NOT EXISTS operon.transaction_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INT NOT NULL,
    output TEXT,
    error TEXT,
    PRIMARY KEY (workflow_uuid, function_id)
  );
`;
