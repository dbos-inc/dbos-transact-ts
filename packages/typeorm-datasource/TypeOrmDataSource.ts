import { PoolConfig, DatabaseError as PGDatabaseError } from 'pg';

interface ExistenceCheck {
  exists: boolean;
}

export const schemaExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`;
export const txnOutputTableExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'knex_transaction_outputs')`;
export const txnOutputIndexExistsQuery = `SELECT EXISTS (SELECT FROM pg_indexes WHERE schemaname='dbos' AND tablename = 'transaction_outputs' AND indexname = 'transaction_outputs_created_at_index')`;

export interface transaction_outputs {
  workflow_id: string;
  function_num: number;
  output: string | null;
}

export const createUserDBSchema = `CREATE SCHEMA IF NOT EXISTS dbos;`;

export const userDBSchema = `
  CREATE TABLE IF NOT EXISTS dbos.knex_transaction_outputs (
    workflow_id TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    PRIMARY KEY (workflow_id, function_num)
  );
`;

export const columnExistsQuery = `
  SELECT EXISTS (
    SELECT FROM information_schema.columns 
    WHERE table_schema = 'dbos' 
      AND table_name = 'transaction_outputs' 
      AND column_name = 'function_name'
  ) AS exists;
`;

export const addColumnQuery = `
  ALTER TABLE dbos.transaction_outputs 
    ADD COLUMN function_name TEXT NOT NULL DEFAULT '';
`;

export const userDBIndex = `
  CREATE INDEX IF NOT EXISTS transaction_outputs_created_at_index ON dbos.transaction_outputs (created_at);
`;
