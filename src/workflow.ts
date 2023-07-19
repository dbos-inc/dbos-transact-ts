/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon, operon__FunctionOutputs } from './operon';
import { Pool, PoolClient } from 'pg';
import { OperonTransaction, TransactionContext } from './transaction';

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

export interface WorkflowParams {
  idempotencyKey?: string;
}

export class WorkflowContext {
  pool: Pool;

  readonly workflowID: string;
  #functionID: number = 0;

  constructor(pool: Pool, workflowID: string) {
    this.pool = pool;
    this.workflowID = workflowID;
  }

  functionIDGetIncrement() : number {
    return this.#functionID++;
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, ...args: T): Promise<R> {
    let client: PoolClient = await this.pool.connect();
    const fCtxt: TransactionContext = new TransactionContext(client, this.functionIDGetIncrement());

    const checkExecution = async () => {
      const { rows } = await client.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
        [this.workflowID, fCtxt.functionID]);
      if (rows.length === 0) {
        return null;
      } else {
        return JSON.parse(rows[0].output) as R;
      }
    }

    const recordExecution = async (output: R) => {
      await client.query("INSERT INTO operon__FunctionOutputs VALUES ($1, $2, $3)",
        [this.workflowID, fCtxt.functionID, JSON.stringify(output)]);
    }

    await client.query("BEGIN");

    // Check if this execution previously happened, returning its original result if it did.
    const check: R | null = await checkExecution();
    if (check !== null) {
      await client.query("ROLLBACK");
      client.release();
      return check;
    }

    // Execute the function.
    const result: R = await txn(fCtxt, ...args);

    // Record the execution, commit, and return.
    if(fCtxt.isAborted()) {
      client = await this.pool.connect();
    }
    await recordExecution(result);
    await client.query("COMMIT");
    client.release();
    return result;
  }
}
