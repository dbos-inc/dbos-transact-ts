/* eslint-disable @typescript-eslint/no-explicit-any */
/*eslint-disable no-constant-condition */
import { operon__FunctionOutputs, operon__Notifications } from './operon';
import { Pool, PoolClient, Notification, DatabaseError } from 'pg';
import { OperonTransaction, TransactionContext } from './transaction';
import { OperonCommunicator, CommunicatorContext, CommunicatorParams } from './communicator';
import { OperonError } from './error';
import { serializeError, deserializeError } from 'serialize-error';
import { User } from './users';

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

export interface WorkflowParams {
  workflowUUID?: string;
  runAs: User;
  id: string;
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
    const { rows } = await client.query<operon__FunctionOutputs>("SELECT output, error FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
      [this.workflowUUID, currFuncID]);
    if (rows.length === 0) {
      return operonNull;
    } else if (JSON.parse(rows[0].error) !== null) {
      throw deserializeError(JSON.parse(rows[0].error));
    } else {
      return JSON.parse(rows[0].output) as R;  // Could be null.
    }
  }

  async recordExecution<R>(client: PoolClient, currFuncID: number, output: R | null, err: Error | null): Promise<void> {
    const serialErr = (err !== null) ? serializeError(err) : null;
    await client.query("INSERT INTO operon__FunctionOutputs (workflow_id, function_id, output, error) VALUES ($1, $2, $3, $4)",
      [this.workflowUUID, currFuncID, JSON.stringify(output), JSON.stringify(serialErr)]);
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, ...args: T): Promise<R> {
    let retryWaitMillis = 1;
    const backoffFactor = 2;
    const funcId = this.functionIDGetIncrement();
    while(true) {
      let client: PoolClient = await this.pool.connect();
      try {
        const fCtxt: TransactionContext = new TransactionContext(client, funcId);

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
        await this.recordExecution(client, fCtxt.functionID, result, null);
        await client.query("COMMIT");
        return result;
      } catch (error) {
        const err: DatabaseError = error as DatabaseError;
        // If the error is a serialization failure, rollback and retry with exponential backoff.
        if (err.code === '40001') { // serialization_failure in PostgreSQL
          await client.query('ROLLBACK');
        }  else {
          // If the error is something else, rollback and re-throw
          await client.query('ROLLBACK');
          await this.recordExecution(client, funcId, null, error as Error);
          throw error;
        }
      } finally {
        client.release();
      }
      await new Promise(resolve => setTimeout(resolve, retryWaitMillis));
      retryWaitMillis *= backoffFactor;
    }
  }

  async external<T extends any[], R>(commFn: OperonCommunicator<T, R>, params: CommunicatorParams, ...args: T): Promise<R> {
    const ctxt: CommunicatorContext = new CommunicatorContext(this.functionIDGetIncrement(), params);
    const client: PoolClient = await this.pool.connect();

    // Check if this execution previously happened, returning its original result if it did.
    try {
      const check: R | OperonNull = await this.checkExecution<R>(client, ctxt.functionID);
      if (check !== operonNull) {
        client.release();
        return check as R; 
      }
    } catch (err) {
      // Throw an error if it originally did.
      client.release();
      throw err;
    }

    // Execute the communicator function.  If it throws an exception, retry with exponential backoff.
    // After reaching the maximum number of retries, throw an OperonError.
    let result: R | OperonNull = operonNull;
    if (!ctxt.retriesAllowed) {
      try {
        result = await commFn(ctxt, ...args);
      } catch (error) {
        // If retries not allowed, record the error and throw it to upper level.
        const err = error as Error;
        await this.recordExecution<R>(client, ctxt.functionID, null, err);
        client.release();
        throw error;
      }
    } else {
      let numAttempts = 0;
      let intervalSeconds = ctxt.intervalSeconds;
      while (result === operonNull && numAttempts++ < ctxt.maxAttempts) {
        try {
          result = await commFn(ctxt, ...args);
        } catch (error) { /* empty */ }
        if (result === operonNull && numAttempts < ctxt.maxAttempts) {
          // Sleep for an interval, then increase the interval by backoffRate.
          await new Promise(resolve => setTimeout(resolve, intervalSeconds * 1000));
          intervalSeconds *= ctxt.backoffRate;
        }
      }
      // TODO: add error logging once we have a logging system.
    }

    if (result === operonNull) {
      // Record error and throw an exception.
      const operonErr: OperonError = new OperonError("Communicator reached maximum retries.", 1);
      await this.recordExecution<R>(client, ctxt.functionID, null, operonErr);
      client.release();
      throw operonErr;
    }
    // Record the execution and return.
    await this.recordExecution<R>(client, ctxt.functionID, result as R, null);
    client.release();
    return result as R;
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
    await this.recordExecution<boolean>(client, functionID, success, null);
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
      await this.recordExecution<T>(client, functionID, message, null);
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
      await this.recordExecution<T>(client, functionID, message, null);
      await client.query(`COMMIT`);
      client.release();
      return message;
    } else {
      await client.query(`ROLLBACK`);
      await this.recordExecution(client, functionID, null, null);
      client.release();
      return null;
    }
  }

}
