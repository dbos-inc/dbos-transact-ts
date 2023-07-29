/* eslint-disable @typescript-eslint/no-explicit-any */
/*eslint-disable no-constant-condition */
import {
  operon__FunctionOutputs,
  operon__Notifications,
  Operon
} from './operon';
import { PoolClient, DatabaseError } from 'pg';
import { OperonTransaction, TransactionContext } from './transaction';
import { OperonCommunicator, CommunicatorContext } from './communicator';
import { OperonError } from './error';
import { serializeError, deserializeError } from 'serialize-error';

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

export interface WorkflowParams {
  workflowUUID?: string;
  runAs?: string;
}

export interface WorkflowConfig {
  rolesThatCanRun?: string[];
}

export interface OperonNull {}
export const operonNull: OperonNull = {};

export class WorkflowContext {
  readonly workflowUUID: string;
  #functionID: number = 0;
  readonly #operon;
  readonly resultBuffer: Map<number, any> = new Map<number, any>();

  constructor(operon: Operon, workflowUUID: string, workflowConfig: WorkflowConfig) {
    void workflowConfig;
    this.#operon = operon;
    this.workflowUUID = workflowUUID;
  }

  functionIDGetIncrement() : number {
    return this.#functionID++;
  }

  async checkExecution<R>(client: PoolClient, currFuncID: number): Promise<R | OperonNull> {
    const { rows } = await client.query<operon__FunctionOutputs>("SELECT output, error FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
      [this.workflowUUID, currFuncID]);
    if (rows.length === 0) {
      return operonNull;
    } else if (JSON.parse(rows[0].error) !== null) {
      const error: any = deserializeError(JSON.parse(rows[0].error));
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      error.__retry__ = true;
      throw error;
    } else {
      return JSON.parse(rows[0].output) as R;  // Could be null.
    }
  }

  async flushResultBuffer(client: PoolClient): Promise<void> {
    try {
      for (const [funcID, output] of this.resultBuffer) {
        await client.query("INSERT INTO operon__FunctionOutputs (workflow_id, function_id, output, error) VALUES ($1, $2, $3, $4)",
          [this.workflowUUID, funcID, JSON.stringify(output), JSON.stringify(null)]);
      }
      this.resultBuffer.clear();
    } catch (err) {
      await client.query('ROLLBACK');
      const error: DatabaseError = err as DatabaseError;
      if (error.code === "40001" || error.code === "23505") {
        const operonError = new OperonError("Conflicting UUIDs");
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        (operonError as any).__retry__ = true; // We know this is a retry because another transaction ran previously with the same UUID.
        throw operonError;
      } else {
        throw err;
      }
    }
  }

  async recordError(client: PoolClient, currFuncID: number, err: Error) {
    const serialErr = serializeError(err);
    try {
      await client.query("INSERT INTO operon__FunctionOutputs (workflow_id, function_id, output, error) VALUES ($1, $2, $3, $4)",
        [this.workflowUUID, currFuncID, JSON.stringify(null), JSON.stringify(serialErr)]);
    } catch (err) {
      await client.query('ROLLBACK');
      const error: DatabaseError = err as DatabaseError;
      if (error.code === "40001" || error.code === "23505") {
        throw new OperonError("Conflicting UUIDs");
      } else {
        throw err;
      }
    }
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, ...args: T): Promise<R> {
    const txnConfig = this.#operon.transactionConfigMap.get(txn);
    if (txnConfig === undefined) {
      throw new OperonError(`Unregistered Transaction ${txn.name}`)
    }
    let retryWaitMillis = 1;
    const backoffFactor = 2;
    const funcId = this.functionIDGetIncrement();
    while(true) {
      let client: PoolClient = await this.#operon.pool.connect();
      try {
        const fCtxt: TransactionContext = new TransactionContext(client, funcId, txnConfig);

        await client.query(`BEGIN ISOLATION LEVEL ${fCtxt.isolationLevel}`);
        if (fCtxt.readOnly) {
          await client.query(`SET TRANSACTION READ ONLY`);
        }

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
          client = await this.#operon.pool.connect();
        }
        this.resultBuffer.set(funcId, result);
        if (!fCtxt.readOnly) {
          await this.flushResultBuffer(client);
        }
        await client.query("COMMIT");
        return result;
      } catch (error) {
        const err: DatabaseError = error as DatabaseError;
        // If the error is a serialization failure, rollback and retry with exponential backoff.
        await client.query('ROLLBACK');
        if (err.code !== '40001') { // serialization_failure in PostgreSQL
          // If the error is something else, record  and re-throw
          if (!Object.hasOwn(err, "__retry__")) {
            await client.query('BEGIN');
            await this.flushResultBuffer(client);
            await this.recordError(client, funcId, error as Error);
            await client.query('COMMIT');
          }
          throw error;
        }
      } finally {
        client.release();
      }
      await new Promise(resolve => setTimeout(resolve, retryWaitMillis));
      retryWaitMillis *= backoffFactor;
    }
  }

  async external<T extends any[], R>(commFn: OperonCommunicator<T, R>, ...args: T): Promise<R> {
    const commConfig = this.#operon.communicatorConfigMap.get(commFn);
    if (commConfig === undefined) {
      throw new OperonError(`Unregistered External ${commFn.name}`)
    }
    const ctxt: CommunicatorContext = new CommunicatorContext(this.functionIDGetIncrement(), commConfig);
    const client: PoolClient = await this.#operon.pool.connect();

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
        await client.query('BEGIN');
        await this.flushResultBuffer(client);
        await this.recordError(client, ctxt.functionID, error as Error);
        await client.query('COMMIT');
        client.release();
        throw error;
      }
    } else {
      let numAttempts = 0;
      let intervalSeconds: number = ctxt.intervalSeconds;
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
      await client.query('BEGIN');
      await this.flushResultBuffer(client);
      await this.recordError(client, ctxt.functionID, operonErr as Error);
      await client.query('COMMIT');
      client.release();
      throw operonErr;
    }
    // Record the execution and return.
    this.resultBuffer.set(ctxt.functionID, result);
    await this.flushResultBuffer(client);
    client.release();
    return result as R;
  }

  async send<T extends NonNullable<any>>(key: string, message: T) : Promise<boolean> {
    const client: PoolClient = await this.#operon.pool.connect();
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
    const success: boolean = (rows.length !== 0);
    this.resultBuffer.set(functionID, success);
    await this.flushResultBuffer(client);
    await client.query("COMMIT");
    client.release();
    return success;
  }

  async recv<T extends NonNullable<any>>(key: string, timeoutSeconds: number) : Promise<T | null> {
    let client = await this.#operon.pool.connect();
    const functionID: number = this.functionIDGetIncrement();

    const check: T | OperonNull = await this.checkExecution<T>(client, functionID);
    if (check !== operonNull) {
      client.release();
      return check as T;
    }

    // First, register the key with the global notifications listener.
    let resolveNotification: () => void;
    const messagePromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });
    this.#operon.listenerMap[key] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously, so this is guaranteed to be defined.
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
      this.resultBuffer.set(functionID, message);
      await this.flushResultBuffer(client);
      await client.query(`COMMIT`);
      client.release();
      return message;
    } else {
      await client.query(`ROLLBACK`);
    }
    client.release();

    // Wait for the notification, then check if the key is in the DB, returning the message if it is and NULL if it isn't.
    await received;
    client = await this.#operon.pool.connect();
    await client.query(`BEGIN`);
    ({ rows } = await client.query<operon__Notifications>("DELETE FROM operon__Notifications WHERE key=$1 RETURNING message", [key]));
    if (rows.length > 0 ) {
      const message = JSON.parse(rows[0].message) as T;
      this.resultBuffer.set(functionID, message);
      await this.flushResultBuffer(client);
      await client.query(`COMMIT`);
      client.release();
      return message;
    } else {
      await client.query(`ROLLBACK`);
      this.resultBuffer.set(functionID, null);
      await this.flushResultBuffer(client);
      client.release();
      return null;
    }
  }

}
