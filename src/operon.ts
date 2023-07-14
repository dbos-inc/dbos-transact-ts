import { Pool, PoolConfig } from 'pg';

export interface operon__FunctionOutputs {
    workflow_id: string;
    function_id: number;
    output: string;
}

export interface operon__IdempotencyKeys {
  idempotency_key: number;
}

export class Operon {
  pool: Pool;
  constructor(config: PoolConfig) {
    this.pool = new Pool(config);
  }

  async initializeOperonTables() {
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__FunctionOutputs (
      workflow_id VARCHAR(64) NOT NULL,
      function_id INT NOT NULL,
      output TEXT NOT NULL,
      PRIMARY KEY (workflow_id, function_id)
      );`
    );
    await this.pool.query(`CREATE SEQUENCE IF NOT EXISTS operon__IdempotencyKeys`);
  }

  async resetOperonTables() {
    await this.pool.query(`DROP TABLE IF EXISTS operon__FunctionOutputs;`);
    await this.pool.query(`DROP SEQUENCE IF EXISTS operon__IdempotencyKeys`);
    await this.initializeOperonTables();
  }
}