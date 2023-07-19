import { Pool, PoolConfig } from 'pg';
import { OperonWorkflow, WorkflowContext, WorkflowParams } from './workflow';
import { v1 as uuidv1 } from 'uuid';

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

  #generateIdempotencyKey(): string {
    return uuidv1();
  }
  

  async workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T) {
      // TODO: need to optimize this extra transaction per workflow.
      const recordExecution = async (input: T) => {
        const workflowFuncId = wCtxt.functionIDGetIncrement();
        const client = await this.pool.connect();
        await client.query("BEGIN;");
        const { rows } = await client.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
          [workflowID, workflowFuncId]);
  
        let retInput: T;
        if (rows.length === 0) {
          // This workflow has never executed before, so record the input
          await client.query("INSERT INTO operon__FunctionOutputs VALUES ($1, $2, $3)",
            [workflowID, workflowFuncId, JSON.stringify(input)]);
          retInput = input;
        } else {
          // Return the old recorded input
          retInput = JSON.parse(rows[0].output) as T;
        }
  
        await client.query("COMMIT");
        client.release();
  
        return retInput;
      }
  
      const workflowID: string = params.idempotencyKey ? params.idempotencyKey : this.#generateIdempotencyKey();
      const wCtxt: WorkflowContext = new WorkflowContext(this.pool, workflowID);
      const input = await recordExecution(args);
      const result: R = await wf(wCtxt, ...input);
      return result;
  }
}