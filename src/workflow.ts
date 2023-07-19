/* eslint-disable @typescript-eslint/no-explicit-any */
import { operon__FunctionOutputs } from './operon';
import { Pool, PoolClient } from 'pg';
import { OperonTransaction, TransactionContext } from './transaction';
import { OperonCommunicator, CommunicatorContext, CommunicatorParams } from './communicator';

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

  async external<T extends any[], R>(commFn: OperonCommunicator<T, R>, params: CommunicatorParams, ...args: T): Promise<R | null> {
    const ctxt: CommunicatorContext = new CommunicatorContext(this.functionIDGetIncrement(), params);

    const checkExecution = async () => {
      const { rows } = await this.pool.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
        [this.workflowID, ctxt.functionID]);
      if (rows.length === 0) {
        return null;
      } else {
        return JSON.parse(rows[0].output) as R;
      }
    }

    const recordExecution = async (output: R | null) => {
      await this.pool.query("INSERT INTO operon__FunctionOutputs VALUES ($1, $2, $3)", 
        [this.workflowID, ctxt.functionID, JSON.stringify(output)]);
    }

    // Check if this execution previously happened, returning its original result if it did.
    const check: R | null = await checkExecution();
    if (check !== null) {
      return check; 
    }

    // Execute the communicator function.
    let result: R | null = null;
    
    if (!params.retriesAllowed) {
      result = await commFn(ctxt, ...args);
    } else {
      let numAttempts = 0;
      let intervalSeconds = ctxt.intervalSeconds;
      while (result == null && numAttempts++ < ctxt.maxAttempts) {
        try {
          result = await commFn(ctxt, ...args);
        } catch (error) { }
        if (result == null && numAttempts < ctxt.maxAttempts) {
          // Sleep for an interval, then increase the interval by backoffRate.
          await new Promise(resolve => setTimeout(resolve, intervalSeconds * 1000));
          intervalSeconds *= ctxt.backoffRate;
        }
      }
    }


    // Record the execution and return.
    await recordExecution(result);
    return result;
  }
}
