/* eslint-disable @typescript-eslint/no-explicit-any */
/*eslint-disable no-constant-condition */
import {
  operon__FunctionOutputs,
  operon__Notifications,
  Operon,
  OperonNull,
  operonNull
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

export class WorkflowContext {
  #functionID: number = 0;
  readonly #operon;
  readonly resultBuffer: Map<number, any> = new Map<number, any>();

  constructor(operon: Operon, readonly workflowUUID: string, readonly workflowConfig: WorkflowConfig) {
    this.#operon = operon;
  }

  functionIDGetIncrement() : number {
    return this.#functionID++;
  }

  /**
   * Check if an operation has already executed in a workflow.
   * This should run before any operation's execution.
   * If it previously executed succesfully, return its output.
   * If it previously executed and threw an error, throw that error.
   * Otherwise, return OperonNull.
   */
  async checkExecution<R>(client: PoolClient, funcID: number): Promise<R | OperonNull> {
    const { rows } = await client.query<operon__FunctionOutputs>("SELECT output, error FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
      [this.workflowUUID, funcID]);
    if (rows.length === 0) {
      return operonNull;
    } else if (JSON.parse(rows[0].error) !== null) {
      await client.query("ROLLBACK");
      client.release();
      throw deserializeError(JSON.parse(rows[0].error));
    } else {
      return JSON.parse(rows[0].output) as R;  // Could be null.
    }
  }

  /**
   * Write all entries in the workflow result buffer to the database.  
   * Additionally write a blank "guard" row protecting the function's ID.
   * This should run at the start of any write transaction (including external, send, and recv).
   * It must be followed in the same transaction by a call to writeOutput or recordError.
   * If it encounters a primary key or serialization error, this indicates a concurrent execution with the same UUID, so throw an OperonError.
   */
  async flushResultBuffer(client: PoolClient): Promise<void> {
    const funcIDs = Array.from(this.resultBuffer.keys());
    funcIDs.sort();
    try {
      for (const funcID of funcIDs) {
        await client.query("INSERT INTO operon__FunctionOutputs (workflow_id, function_id, output, error) VALUES ($1, $2, $3, $4)",
          [this.workflowUUID, funcID, JSON.stringify(this.resultBuffer.get(funcID)), JSON.stringify(null)]);
      } 
    } catch (error) {
      await client.query('ROLLBACK');
      client.release();
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '40001' || err.code === '23505') { // Serialization and primary key conflict (Postgres).
        throw new OperonError("Conflicting UUIDs", 5); // TODO: Break out of the workflow.
      } else {
        throw err;
      }
    }
  }

  /**
   * Write a function's output to the database.
   * This should run immediately before committing a successful write transaction.
   */
  async writeOutput<R>(client: PoolClient, funcID: number, output: R): Promise<void> {
    await client.query("UPDATE operon__FunctionOutputs SET output=$1 WHERE workflow_id=$2 AND function_id=$3;",
      [JSON.stringify(output), this.workflowUUID, funcID]);
  }

  /**
   * Record an error to the database.
   * This should run during error handling after flushing the buffer.
   */
  async recordError(client: PoolClient, funcID: number, err: Error) {
    const serialErr = JSON.stringify(serializeError(err));
    await client.query("UPDATE operon__FunctionOutputs SET error=$1 WHERE workflow_id=$2 AND function_id=$3;",
      [serialErr, this.workflowUUID, funcID]);
  }

  /**
   * Execute a transactional function.
   * The transaction is guaranteed to execute exactly once, even if the workflow is retried with the same UUID.
   * If the transaction encounters a Postgres serialization error, retry it.
   * If it encounters any other error, throw it.
   */
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
      const fCtxt: TransactionContext = new TransactionContext(client, funcId, txnConfig);

      await client.query(`BEGIN ISOLATION LEVEL ${fCtxt.isolationLevel}`);
      if (fCtxt.readOnly) {
        await client.query(`SET TRANSACTION READ ONLY`);
      }
        
      // Check if this execution previously happened, returning its original result if it did.
      const check: R | OperonNull = await this.checkExecution<R>(client, fCtxt.functionID);
      if (check !== operonNull) {
        await client.query("ROLLBACK");
        client.release();
        return check as R;
      }
      // Flush the result buffer, setting a placeholder entry with the function's ID to block concurrent executions with the same UUID.
      this.resultBuffer.set(funcId, null);
      if (!fCtxt.readOnly) {
        await this.flushResultBuffer(client);
      }

      let result: R;
      try {
        // Execute the function.
        result = await txn(fCtxt, ...args);
      } catch (error) {
        const err: DatabaseError = error as DatabaseError;
        // If the error is a serialization failure, rollback and retry with exponential backoff.
        await client.query('ROLLBACK');
        if (err.code === '40001') { // serialization_failure in PostgreSQL
          // Retry serialization failures
          client.release();
          await new Promise(resolve => setTimeout(resolve, retryWaitMillis));
          retryWaitMillis *= backoffFactor;
          continue;
        } else {
          // Record and re-throw other errors
          await client.query('BEGIN');
          await this.flushResultBuffer(client);
          await this.recordError(client, funcId, error as Error);
          await client.query('COMMIT');
          this.resultBuffer.clear();
          client.release();
          throw error;
        }
      }

      // Record the execution, commit, and return.
      if (fCtxt.readOnly) {
        // Buffer the output of read-only transactions instead of synchronously writing it.
        this.resultBuffer.set(funcId, result);
        if (!fCtxt.isAborted()) {
          await client.query("COMMIT");
          client.release();
        }
      } else {
        if (fCtxt.isAborted()) {
          client = await this.#operon.pool.connect();
          await client.query("BEGIN");
          await this.flushResultBuffer(client);
        }
        // Synchronously record the output of write transactions.
        await this.writeOutput<R>(client, funcId, result);
        await client.query("COMMIT");
        this.resultBuffer.clear();
        client.release();
      }
      return result;
    }
  }

  /**
   * Execute a communicator function.
   * If it encounters any error, retry according to its configured retry policy until the maximum number of attempts is reached, then throw an OperonError.
   * The communicator may execute many times, but once it is complete (either by succeeding or throwing an OperonError), it will not re-execute.
   */
  async external<T extends any[], R>(commFn: OperonCommunicator<T, R>, ...args: T): Promise<R> {
    const commConfig = this.#operon.communicatorConfigMap.get(commFn);
    if (commConfig === undefined) {
      throw new OperonError(`Unregistered External ${commFn.name}`)
    }
    const ctxt: CommunicatorContext = new CommunicatorContext(this.functionIDGetIncrement(), commConfig);
    let client: PoolClient = await this.#operon.pool.connect();

    // Check if this execution previously happened, returning its original result if it did.
    const check: R | OperonNull = await this.checkExecution<R>(client, ctxt.functionID);
    client.release();
    if (check !== operonNull) {
      return check as R; 
    }

    // Execute the communicator function.  If it throws an exception, retry with exponential backoff.
    // After reaching the maximum number of retries, throw an OperonError.
    let result: R | OperonNull = operonNull;
    let err: Error | OperonNull = operonNull;
    if (ctxt.retriesAllowed) {
      let numAttempts = 0;
      let intervalSeconds: number = ctxt.intervalSeconds;
      while (result === operonNull && numAttempts++ < ctxt.maxAttempts) {
        try {
          result = await commFn(ctxt, ...args);
        } catch (error) { 
          if (numAttempts < ctxt.maxAttempts) {
            // Sleep for an interval, then increase the interval by backoffRate.
            await new Promise(resolve => setTimeout(resolve, intervalSeconds * 1000));
            intervalSeconds *= ctxt.backoffRate;
          }
        }
      }
    } else {
      try {
        result = await commFn(ctxt, ...args);
      } catch (error) {
        err = error as Error;
      }
    }

    client = await this.#operon.pool.connect();
    if (result === operonNull) {
      // Record the error, then throw it.
      err = err === operonNull ? new OperonError("Communicator reached maximum retries.", 1) : err;
      await client.query('BEGIN');
      this.resultBuffer.set(ctxt.functionID, null);
      await this.flushResultBuffer(client);
      await this.recordError(client, ctxt.functionID, err as Error);
      await client.query('COMMIT');
      this.resultBuffer.clear();
      client.release();
      throw err;
    } else {
      // Record the execution and return.
      await client.query('BEGIN');
      this.resultBuffer.set(ctxt.functionID, result);
      await this.flushResultBuffer(client);
      await client.query('COMMIT');
      this.resultBuffer.clear();
      client.release();
      return result as R;
    }
  }

  /**
   * Send a message to a key.
   * If no message is associated with the key, return true.
   * If a message is associated with the key, do nothing and return false.
   */
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
    this.resultBuffer.set(functionID, null);
    await this.flushResultBuffer(client);
    const { rows }  = await client.query(`INSERT INTO operon__Notifications (key, message) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING RETURNING 'Success';`,
      [key, JSON.stringify(message)])
    const success: boolean = (rows.length !== 0); // Return true if successful, false if the key already exists.
    await this.writeOutput(client, functionID, success);
    await client.query("COMMIT");
    this.resultBuffer.clear();
    client.release();
    return success;
  }

  /**
   * Receive and consume a message from a key, returning the message.
   * Waits until the message arrives or a timeout is reached.
   * If the timeout is reached, return null.
   */
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
    this.resultBuffer.set(functionID, null);
    await this.flushResultBuffer(client);
    let { rows } = await client.query<operon__Notifications>("DELETE FROM operon__Notifications WHERE key=$1 RETURNING message", [key]);
    if (rows.length > 0 ) {
      const message: T = JSON.parse(rows[0].message) as T;
      await this.writeOutput(client, functionID, message);
      await client.query(`COMMIT`);
      this.resultBuffer.clear();
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
    this.resultBuffer.set(functionID, null);
    await this.flushResultBuffer(client);
    ({ rows } = await client.query<operon__Notifications>("DELETE FROM operon__Notifications WHERE key=$1 RETURNING message", [key]));
    let message: T | null = null;
    if (rows.length > 0 ) {
      message = JSON.parse(rows[0].message) as T;
    }
    await this.writeOutput(client, functionID, message);
    await client.query(`COMMIT`);
    this.resultBuffer.clear();
    client.release();
    return message;
  }

}
