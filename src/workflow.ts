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

    // Execute the communicator function.  If it throws an exception or returns null, retry with exponential backoff.
    // After reaching the maximum number of retries, return null.
    let result: R | null = null;
    if (!ctxt.retriesAllowed) {
      try {
        result = await commFn(ctxt, ...args);
      } catch (error) { /* empty */ }
    } else {
      let numAttempts = 0;
      let intervalSeconds = ctxt.intervalSeconds;
      while (result === null && numAttempts++ < ctxt.maxAttempts) {
        try {
          result = await commFn(ctxt, ...args);
        } catch (error) { /* empty */ }
        if (result === null && numAttempts < ctxt.maxAttempts) {
          // Sleep for an interval, then increase the interval by backoffRate.
          await new Promise(resolve => setTimeout(resolve, intervalSeconds * 1000));
          intervalSeconds *= ctxt.backoffRate;
        }
      }
      // TODO: add error logging once we have a logging system.
    }

    // Record the execution and return.
    await recordExecution(result);
    return result;
  }

  async send(key: string, message: any) : Promise<boolean> {
    const client: PoolClient = await this.pool.connect();
    const functionID: number = this.functionIDGetIncrement();

    const checkExecution = async () => {
      const { rows } = await client.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
        [this.workflowID, functionID]);
      if (rows.length === 0) {
        return null;
      } else {
        return JSON.parse(rows[0].output) as boolean;
      }
    }

    const recordExecution = async (output: boolean) => {
      await client.query("INSERT INTO operon__FunctionOutputs VALUES ($1, $2, $3)",
        [this.workflowID, functionID, JSON.stringify(output)]);
    }
    
    await client.query("BEGIN");
    const check: boolean | null = await checkExecution();
    if (check !== null) {
      await client.query("ROLLBACK");
      client.release();
      return check;
    }
    const { rows }  = await client.query(`INSERT INTO operon__Notifications (key, message) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING RETURNING 'Exists';`,
      [key, message])
    // Return true if successful, false if key already exists.
    const success: boolean = rows.length === 0;
    recordExecution(success);
    await client.query("COMMIT");
    client.release();
    return rows.length === 0;
  }

  async recv(key: string, timeoutSeconds: number) : Promise<any | null> {
    const client = await this.pool.connect();
    const functionID: number = this.functionIDGetIncrement();


    const checkExecution = async () => {
      const { rows } = await client.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
        [this.workflowID, functionID]);
      if (rows.length === 0) {
        return null;
      } else {
        return JSON.parse(rows[0].output);
      }
    }

    const recordExecution = async (output: any) => {
      await client.query("INSERT INTO operon__FunctionOutputs VALUES ($1, $2, $3)",
        [this.workflowID, functionID, JSON.stringify(output)]);
    }

    const check: any | null = await checkExecution();
    if (check !== null) {
      client.release();
      return check;
    }

    // Poll the database once a second until the notification has been received or the timeout is reached.
    // TODO: Do this less naively.  Use triggers maybe???
    let elapsed = 0;
    do {
      await client.query(`BEGIN`);
      const { rows } = await client.query("DELETE FROM operon__Notifications WHERE key=$1 RETURNING message", [key]);
      if (rows.length > 0 ) {
        const message = rows[0].message;
        recordExecution(message);
        await client.query(`COMMIT`);
        client.release();
        return message;
      } else {
        await client.query(`ROLLBACK`);
        elapsed += 1;
        if (elapsed <= timeoutSeconds) {
          await new Promise(resolve => setTimeout(resolve, 1000)); // Sleep 1 second.
        }
      }
    } while (elapsed <= timeoutSeconds)

    client.release();
    return null;
  }
}
