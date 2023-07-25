/* eslint-disable @typescript-eslint/no-explicit-any */
/*eslint-disable no-constant-condition */
import { operon__FunctionOutputs, operon__Notifications } from './operon';
import { Pool, PoolClient, Notification, DatabaseError } from 'pg';
import { OperonTransaction, TransactionContext } from './transaction';
import { OperonCommunicator, CommunicatorContext, CommunicatorParams } from './communicator';

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

export interface WorkflowParams {
  workflowUUID?: string;
}

interface OperonNull {}
const operonNull: OperonNull = {};

export class WorkflowContext {
  pool: Pool;

  readonly workflowUUID: string;
  #functionID: number = 0;

  constructor(pool: Pool, workflowUUID: string) {
    this.pool = pool;
    this.workflowUUID = workflowUUID;
  }

  functionIDGetIncrement() : number {
    return this.#functionID++;
  }

  async checkExecution<R>(client: PoolClient, currFuncID: number): Promise<R | OperonNull> {
    // TODO: read errors.
    const { rows } = await client.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
      [this.workflowUUID, currFuncID]);
    if (rows.length === 0) {
      return operonNull;
    } else {
      return JSON.parse(rows[0].output) as R;  // Could be null.
    }
  }

  async recordExecution<R>(client: PoolClient, currFuncID: number, output: R): Promise<void> {
    // TODO: record errors.
    await client.query("INSERT INTO operon__FunctionOutputs VALUES ($1, $2, $3)",
      [this.workflowUUID, currFuncID, JSON.stringify(output)]);
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, ...args: T): Promise<R> {
    let retryWaitMillis = 1;
    const backoffFactor = 2;
    while(true) {
      let client: PoolClient = await this.pool.connect();
      try {
        const fCtxt: TransactionContext = new TransactionContext(client, this.functionIDGetIncrement());

        await client.query("BEGIN ISOLATION LEVEL SERIALIZABLE");

        // Check if this execution previously happened, returning its original result if it did.
        const check: R | OperonNull = await this.checkExecution<R>(client, fCtxt.functionID);
        if (check !== operonNull) {
          await client.query("ROLLBACK");
          return check as R;
        }

        // Execute the function.
        const result: R = await txn(fCtxt, ...args);

        // Record the execution, commit, and return.
        if(fCtxt.isAborted()) {
          client = await this.pool.connect();
        }
        await this.recordExecution(client, fCtxt.functionID, result);
        await client.query("COMMIT");
        return result;
      } catch (error) {
        const err: DatabaseError = error as DatabaseError;
        // If the error is a serialization failure, rollback and retry with exponential backoff.
        if (err.code === '40001') { // serialization_failure in PostgreSQL
          await client.query('ROLLBACK')
        }  else {
          // If the error is something else, rollback and re-throw
          await client.query('ROLLBACK')
          throw error
        }
      } finally {
        client.release();
      }
      await new Promise(resolve => setTimeout(resolve, retryWaitMillis));
      retryWaitMillis *= backoffFactor;
    }
  }

  async external<T extends any[], R>(commFn: OperonCommunicator<T, R>, params: CommunicatorParams, ...args: T): Promise<R | null> {
    const ctxt: CommunicatorContext = new CommunicatorContext(this.functionIDGetIncrement(), params);
    const client: PoolClient = await this.pool.connect();

    // Check if this execution previously happened, returning its original result if it did.
    const check: R | OperonNull = await this.checkExecution<R>(client, ctxt.functionID);
    if (check !== operonNull) {
      client.release();
      return check as R; 
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
    await this.recordExecution<R | null>(client, ctxt.functionID, result);
    client.release();
    return result;
  }

  async send<T extends NonNullable<any>>(key: string, message: T) : Promise<boolean> {
    const client: PoolClient = await this.pool.connect();
    const functionID: number = this.functionIDGetIncrement();
    
    await client.query("BEGIN");
    const check: boolean | OperonNull = await this.checkExecution<boolean>(client, functionID);
    if (check !== operonNull) {
      await client.query("ROLLBACK");
      client.release();
      return check as boolean;
    }
    const { rows }  = await client.query(`INSERT INTO operon__Notifications (key, message) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING RETURNING 'Success';`,
      [key, JSON.stringify(message)])
    // Return true if successful, false if key already exists.
    const success: boolean = rows.length === 0;
    await this.recordExecution<boolean>(client, functionID, success);
    await client.query("COMMIT");
    client.release();
    return rows.length !== 0;
  }

  async recv<T extends NonNullable<any>>(key: string, timeoutSeconds: number) : Promise<T | null> {
    const client = await this.pool.connect();
    const functionID: number = this.functionIDGetIncrement();

    const check: T | OperonNull = await this.checkExecution<T>(client, functionID);
    if (check !== operonNull) {
      client.release();
      return check as T;
    }

    // First, set up a channel waiting for a notification from the trigger on the key (or timeout).
    await client.query('LISTEN operon__notificationschannel;');
    let resolveNotification: () => void;
    const messagePromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });
    const handler = (msg: Notification ) => {
      if (msg.payload === key) {
        client.removeListener('notification', handler);
        resolveNotification();
      }
    };
    client.on('notification', handler);
    const timeoutPromise = new Promise<void>((resolve) => {
      setTimeout(() => {
        resolve();
      }, timeoutSeconds * 1000);
    });
    const received = Promise.race([messagePromise, timeoutPromise]);

    // Then, check if the key is already in the DB, returning it if it is.
    await client.query(`BEGIN`);
    let { rows } = await client.query<operon__Notifications>("DELETE FROM operon__Notifications WHERE key=$1 RETURNING message", [key]);
    if (rows.length > 0 ) {
      const message: T = JSON.parse(rows[0].message) as T;
      await this.recordExecution<T>(client, functionID, message);
      await client.query(`COMMIT`);
      client.release();
      return message;
    } else {
      await client.query(`ROLLBACK`);
    }

    // Wait for the notification, then check if the key is in the DB, returning the message if it is and NULL if it isn't.
    await received;
    await client.query(`BEGIN`);
    ({ rows } = await client.query<operon__Notifications>("DELETE FROM operon__Notifications WHERE key=$1 RETURNING message", [key]));
    if (rows.length > 0 ) {
      const message = JSON.parse(rows[0].message) as T;
      await this.recordExecution<T>(client, functionID, message);
      await client.query(`COMMIT`);
      client.release();
      return message;
    } else {
      await client.query(`ROLLBACK`);
      await this.recordExecution(client, functionID, null);
      client.release();
      return null;
    }
  }

}
