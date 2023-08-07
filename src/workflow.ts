/* eslint-disable @typescript-eslint/no-explicit-any */
/*eslint-disable no-constant-condition */
import {
  function_outputs,
  notifications,
  Operon,
  OperonNull,
  operonNull,
  workflow_status
} from './operon';
import { PoolClient, DatabaseError, Pool } from 'pg';
import { OperonTransaction, TransactionContext } from './transaction';
import { OperonCommunicator, CommunicatorContext } from './communicator';
import { OperonError, OperonTopicPermissionDeniedError, OperonWorkflowConflictUUIDError } from './error';
import { serializeError, deserializeError } from 'serialize-error';
import { sleep } from './utils';

const defaultWorkflowReceiveTimeout = 60; // seconds

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

export interface WorkflowParams {
  workflowUUID?: string;
  runAs?: string;
}

export interface WorkflowConfig {
  rolesThatCanRun?: string[];
}

export const WorkflowStatus = {
  PENDING: "PENDING",
  SUCCESS: "SUCCESS",
  ERROR: "ERROR",
} as const;

export class WorkflowContext {
  #functionID: number = 0;
  readonly #operon;
  readonly resultBuffer: Map<number, any> = new Map<number, any>();
  readonly isTempWorkflow: boolean;

  constructor(
    operon: Operon,
    readonly workflowUUID: string,
    readonly runAs: string,
    readonly workflowConfig: WorkflowConfig,
    readonly workflowName: string) {
    this.#operon = operon;
    this.isTempWorkflow = operon.tempWorkflowName === workflowName;
  }

  functionIDGetIncrement() : number {
    return this.#functionID++;
  }

  /**
   * Check if an operation has already executed in a workflow.
   * If it previously executed succesfully, return its output.
   * If it previously executed and threw an error, throw that error.
   * Otherwise, return OperonNull.
   */
  async checkExecution<R>(client: PoolClient, funcID: number): Promise<R | OperonNull> {
    const { rows } = await client.query<function_outputs>("SELECT output, error FROM operon.function_outputs WHERE workflow_uuid=$1 AND function_id=$2",
      [this.workflowUUID, funcID]);
    if (rows.length === 0) {
      return operonNull;
    } else if (JSON.parse(rows[0].error) !== null) {
      await client.query("ROLLBACK");
      client.release();
      throw deserializeError(JSON.parse(rows[0].error));
    } else {
      return JSON.parse(rows[0].output) as R;
    }
  }

  /**
   * Write all entries in the workflow result buffer to the database.
   * Also update workflow status to PENDING.
   * If it encounters a primary key or serialization error, this indicates a concurrent execution with the same UUID, so throw an OperonError.
   */
  async flushResultBuffer(client: PoolClient): Promise<void> {
    const funcIDs = Array.from(this.resultBuffer.keys());
    funcIDs.sort();
    try {
      for (const funcID of funcIDs) {
        await client.query("INSERT INTO operon.function_outputs (workflow_uuid, function_id, output, error) VALUES ($1, $2, $3, $4);",
          [this.workflowUUID, funcID, JSON.stringify(this.resultBuffer.get(funcID)), JSON.stringify(null)]);
      }
      // Update workflow PENDING status.
      if (!this.isTempWorkflow) {
        const { rows } = await client.query<workflow_status>(`INSERT INTO operon.workflow_status (workflow_uuid, workflow_name, status) VALUES ($1, $2, $3)
         ON CONFLICT (workflow_uuid) DO UPDATE SET updated_at_epoch_ms=(EXTRACT(EPOCH FROM now())*1000)::bigint
        RETURNING (SELECT old.status FROM operon.workflow_status old WHERE old.workflow_uuid=$1) AS status;`,
        [this.workflowUUID, this.workflowName, WorkflowStatus.PENDING]);
        if ((rows[0].status === WorkflowStatus.ERROR) || (rows[0].status === WorkflowStatus.SUCCESS)) {
          throw new OperonWorkflowConflictUUIDError();
        }
      }
    } catch (error) {
      await client.query('ROLLBACK');
      client.release();
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '40001' || err.code === '23505') { // Serialization and primary key conflict (Postgres).
        throw new OperonWorkflowConflictUUIDError();
      } else {
        throw err;
      }
    }
  }

  /**
   * Buffer a placeholder value to guard an operation against concurrent executions with the same UUID.
   */
  guardOperation(funcID: number) {
    this.resultBuffer.set(funcID, null);
  }

  /**
   * Write a guarded operation's output to the database.
   */
  async recordGuardedOutput<R>(client: PoolClient, funcID: number, output: R): Promise<void> {
    const serialOutput = JSON.stringify(output);
    await client.query("UPDATE operon.function_outputs SET output=$1 WHERE workflow_uuid=$2 AND function_id=$3;",
      [serialOutput, this.workflowUUID, funcID]);
    if (this.isTempWorkflow) {
      await client.query("INSERT INTO operon.workflow_status (workflow_uuid, workflow_name, status, output) VALUES ($1, $2, $3, $4);",
        [this.workflowUUID, this.workflowName, WorkflowStatus.SUCCESS, serialOutput]);
    }
  }

  /**
   * Record an error in a guarded operation to the database.
   */
  async recordGuardedError(client: PoolClient, funcID: number, err: Error) {
    const serialErr = JSON.stringify(serializeError(err));
    await client.query("UPDATE operon.function_outputs SET error=$1 WHERE workflow_uuid=$2 AND function_id=$3;",
      [serialErr, this.workflowUUID, funcID]);
    if (this.isTempWorkflow) {
      await client.query("INSERT INTO operon.workflow_status (workflow_uuid, workflow_name, status, error) VALUES ($1, $2, $3, $4);",
        [this.workflowUUID, this.workflowName, WorkflowStatus.ERROR, serialErr]);
    }
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
      // Flush the result buffer, setting a guard to block concurrent executions with the same UUID.
      this.guardOperation(funcId);
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
          await sleep(retryWaitMillis);
          retryWaitMillis *= backoffFactor;
          continue;
        } else {
          // Record and throw other errors
          await client.query('BEGIN');
          await this.flushResultBuffer(client);
          await this.recordGuardedError(client, funcId, error as Error);
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
        await this.recordGuardedOutput<R>(client, funcId, result);
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
   * The communicator may execute many times, but once it is complete, it will not re-execute.
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
            await sleep(intervalSeconds);
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
    // `result` can only be operonNull when the communicator timed out
    if (result === operonNull) {
      // Record the error, then throw it.
      err = err === operonNull ? new OperonError("Communicator reached maximum retries.", 1) : err;
      await client.query('BEGIN');
      this.guardOperation(ctxt.functionID);
      await this.flushResultBuffer(client);
      await this.recordGuardedError(client, ctxt.functionID, err as Error);
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
   * Send a message to a key, returning true if successful.
   * If a message is already associated with the key, do nothing and return false.
   */
  async send<T extends NonNullable<any>>(topic: string, key: string, message: T) : Promise<boolean> {
    const functionID: number = this.functionIDGetIncrement();

    // Is this receiver permitted to read from this topic?
    const hasTopicPermissions: boolean = this.hasTopicPermissions(topic);
    if (!hasTopicPermissions) {
      throw new OperonTopicPermissionDeniedError(topic, this.workflowUUID, functionID, this.runAs);
    }

    const client: PoolClient = await this.#operon.pool.connect();

    await client.query("BEGIN");
    const check: boolean | OperonNull = await this.checkExecution<boolean>(client, functionID);
    if (check !== operonNull) {
      await client.query("ROLLBACK");
      client.release();
      return check as boolean;
    }
    this.guardOperation(functionID);
    await this.flushResultBuffer(client);
    const { rows } = await client.query(`INSERT INTO operon.notifications (topic, key, message) VALUES ($1, $2, $3) 
    ON CONFLICT (topic, key) DO NOTHING RETURNING 'Success';`,
    [topic, key, JSON.stringify(message)])
    const success: boolean = (rows.length !== 0); // Return true if successful, false if the key already exists.
    await this.recordGuardedOutput(client, functionID, success);
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
  async recv<T extends NonNullable<any>>(topic: string, key: string, timeoutSeconds: number = defaultWorkflowReceiveTimeout) : Promise<T | null> {
    const functionID: number = this.functionIDGetIncrement();

    // Is this receiver permitted to read from this topic?
    const hasTopicPermissions: boolean = this.hasTopicPermissions(topic);
    if (!hasTopicPermissions) {
      throw new OperonTopicPermissionDeniedError(topic, this.workflowUUID, functionID, this.runAs);
    }

    let client = await this.#operon.pool.connect();

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
    this.#operon.listenerMap[`${topic}::${key}`] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.
    let timer: NodeJS.Timeout;
    const timeoutPromise = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        resolve();
      }, timeoutSeconds * 1000);
    });
    const received = Promise.race([messagePromise, timeoutPromise]);

    // Then, check if the key is already in the DB, returning it if it is.
    await client.query(`BEGIN`);
    this.guardOperation(functionID);
    await this.flushResultBuffer(client);
    let { rows } = await client.query<notifications>("DELETE FROM operon.notifications WHERE topic=$1 AND key=$2 RETURNING message", [topic, key]);
    if (rows.length > 0 ) {
      const message: T = JSON.parse(rows[0].message) as T;
      await this.recordGuardedOutput(client, functionID, message);
      await client.query(`COMMIT`);
      this.resultBuffer.clear();
      client.release();
      delete this.#operon.listenerMap[`${topic}::${key}`];
      return message;
    } else {
      await client.query(`ROLLBACK`);
    }
    client.release();

    // Wait for the notification, then check if the key is in the DB, returning the message if it is and null if it isn't.
    await received;
    clearTimeout(timer!);
    client = await this.#operon.pool.connect();
    await client.query(`BEGIN`);
    this.guardOperation(functionID);
    await this.flushResultBuffer(client);
    ({ rows } = await client.query<notifications>("DELETE FROM operon.notifications WHERE topic=$1 AND key=$2 RETURNING message", [topic, key]));
    let message: T | null = null;
    if (rows.length > 0 ) {
      message = JSON.parse(rows[0].message) as T;
    }
    await this.recordGuardedOutput(client, functionID, message);
    await client.query(`COMMIT`);
    this.resultBuffer.clear();
    client.release();
    return message;
  }

  hasTopicPermissions(requestedTopic: string): boolean {
    const topicAllowedRoles = this.#operon.topicConfigMap.get(requestedTopic);
    if (topicAllowedRoles === undefined) {
      throw new OperonError(`unregistered topic: ${requestedTopic}`);
    }
    if (topicAllowedRoles.length === 0) {
      return true;
    }
    return topicAllowedRoles.includes(this.runAs);
  }
}

export interface WorkflowHandle<R> {
  getStatus(): Promise<string>;
  getResult(): Promise<R>;
  getWorkflowUUID(): string;
}

/**
 * The handle returned when invoking a workflow with Operon.workflow
 */
export class InvokedHandle<R> implements WorkflowHandle<R> {
  constructor(readonly pool: Pool, readonly workflowPromise: Promise<R>, readonly workflowUUID: string) {}

  getWorkflowUUID(): string {
    return this.workflowUUID;
  }

  async getStatus(): Promise<string> {
    const { rows } = await this.pool.query<workflow_status>("SELECT status FROM operon.workflow_status WHERE workflow_uuid=$1", [this.workflowUUID]);
    if (rows.length === 0) {
      return WorkflowStatus.PENDING;
    }
    return rows[0].status;
  }

  async getResult(): Promise<R> {
    return this.workflowPromise;
  }
}

/**
 * The handle returned when retrieving a workflow with Operon.retrieve
 */
export class RetrievedHandle<R> implements WorkflowHandle<R> {
  constructor(readonly pool: Pool, readonly workflowUUID: string) {}

  static readonly pollingIntervalMs: number = 1000;

  getWorkflowUUID(): string {
    return this.workflowUUID;
  }

  async getStatus(): Promise<string> {
    const { rows } = await this.pool.query<workflow_status>("SELECT status FROM operon.workflow_status WHERE workflow_uuid=$1", [this.workflowUUID]);
    if (rows.length === 0) {
      throw new OperonError("UNREACHABLE: Workflow does not exist");
    }
    return rows[0].status;
  }

  async getResult(): Promise<R> {
    while(true) {
      const { rows } = await this.pool.query<workflow_status>("SELECT status, output, error FROM operon.workflow_status WHERE workflow_uuid=$1", [this.workflowUUID]);
      if (rows.length === 0) { 
        throw new OperonError("UNREACHABLE: Workflow does not exist");
      }
      const status = rows[0].status;
      if (status === WorkflowStatus.SUCCESS) {
        return JSON.parse(rows[0].output) as R;
      } else if (status === WorkflowStatus.ERROR) {
        throw deserializeError(JSON.parse(rows[0].error));
      }
      await sleep(RetrievedHandle.pollingIntervalMs);
    }
  }
}