import { Pool, PoolConfig } from 'pg';

export interface operon__FunctionOutputs {
    workflow_id: string;
    function_id: number;
    output: string;
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
  }

  async resetOperonTables() {
    await this.pool.query(`DROP TABLE IF EXISTS operon__FunctionOutputs;`);
    await this.initializeOperonTables();
  }
}