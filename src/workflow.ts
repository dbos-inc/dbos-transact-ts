/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon, OperonNull, operonNull } from "./operon";
import { transaction_outputs } from "../schemas/user_db_schema";
import { OperonTransaction, TransactionContext } from "./transaction";
import { OperonCommunicator, CommunicatorContext } from "./communicator";
import { OperonError, OperonTopicPermissionDeniedError, OperonWorkflowConflictUUIDError } from "./error";
import { serializeError, deserializeError } from "serialize-error";
import { sleep } from "./utils";
import { SystemDatabase } from "./system_database";
import { UserDatabaseClient } from "./user_database";
import { SpanStatusCode } from "@opentelemetry/api";
import { Span } from "@opentelemetry/sdk-trace-base";

const defaultRecvTimeoutSec = 60;

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

export interface WorkflowParams {
  workflowUUID?: string;
  runAs?: string;
  parentSpan?: Span;
}

export interface WorkflowConfig {
  rolesThatCanRun?: string[];
}

export interface WorkflowStatus {
  status: string;
  updatedAtEpochMs: number;
}

export const StatusString = {
  UNKNOWN: "UNKNOWN",
  SUCCESS: "SUCCESS",
  ERROR: "ERROR",
} as const;

export class WorkflowContext {
  functionID: number = 0;
  readonly #operon;
  readonly span: Span;
  readonly resultBuffer: Map<number, any> = new Map<number, any>();
  readonly isTempWorkflow: boolean;
  readonly operationName: string;
  readonly runAs: string;

  constructor(operon: Operon, params: WorkflowParams, readonly workflowUUID: string, readonly workflowConfig: WorkflowConfig, readonly workflowName: string) {
    this.operationName = workflowName;
    this.#operon = operon;
    this.isTempWorkflow = operon.tempWorkflowName === workflowName;
    this.runAs = params.runAs || "defaultRole"; // runAs should have been resolved in operon.workflow()
    this.span = operon.tracer.startSpan(workflowName, params.parentSpan);
    this.span.setAttributes({
      workflowUUID: workflowUUID,
      operationName: workflowName,
      runAs: params.runAs,
      functionID: 0,
    });
  }

  functionIDGetIncrement(): number {
    return this.functionID++;
  }

  log(severity: string, message: string): void {
    this.#operon.logger.log(this, severity, message);
  }

  /**
   * Check if an operation has already executed in a workflow.
   * If it previously executed succesfully, return its output.
   * If it previously executed and threw an error, throw that error.
   * Otherwise, return OperonNull.
   */
  async checkExecution<R>(client: UserDatabaseClient, funcID: number): Promise<R | OperonNull> {
    const rows = await this.#operon.userDatabase.queryWithClient<transaction_outputs>(
      client,
      "SELECT output, error FROM operon.transaction_outputs WHERE workflow_uuid=$1 AND function_id=$2",
      this.workflowUUID,
      funcID
    );
    if (rows.length === 0) {
      return operonNull;
    } else if (JSON.parse(rows[0].error) !== null) {
      throw deserializeError(JSON.parse(rows[0].error));
    } else {
      return JSON.parse(rows[0].output) as R;
    }
  }

  /**
   * Write all entries in the workflow result buffer to the database.
   * If it encounters a primary key or serialization error, this indicates a concurrent execution with the same UUID, so throw an OperonError.
   */
  async flushResultBuffer(client: UserDatabaseClient): Promise<void> {
    const funcIDs = Array.from(this.resultBuffer.keys());
    funcIDs.sort();
    try {
      for (const funcID of funcIDs) {
        await this.#operon.userDatabase.queryWithClient(
          client,
          "INSERT INTO operon.transaction_outputs (workflow_uuid, function_id, output, error) VALUES ($1, $2, $3, $4);",
          this.workflowUUID,
          funcID,
          JSON.stringify(this.resultBuffer.get(funcID)),
          JSON.stringify(null)
        );
      }
    } catch (error) {
      const code = this.#operon.userDatabase.getPostgresErrorCode(error);
      if (code === "40001" || code === "23505") {
        // Serialization and primary key conflict (Postgres).
        throw new OperonWorkflowConflictUUIDError(this.workflowUUID);
      } else {
        throw error;
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
  async recordGuardedOutput<R>(client: UserDatabaseClient, funcID: number, output: R): Promise<void> {
    const serialOutput = JSON.stringify(output);
    await this.#operon.userDatabase.queryWithClient(client, "UPDATE operon.transaction_outputs SET output=$1 WHERE workflow_uuid=$2 AND function_id=$3;", serialOutput, this.workflowUUID, funcID);
  }

  /**
   * Record an error in a guarded operation to the database.
   */
  async recordGuardedError(client: UserDatabaseClient, funcID: number, err: Error) {
    const serialErr = JSON.stringify(serializeError(err));
    await this.#operon.userDatabase.queryWithClient(client, "UPDATE operon.transaction_outputs SET error=$1 WHERE workflow_uuid=$2 AND function_id=$3;", serialErr, this.workflowUUID, funcID);
  }

  /**
   * Execute a transactional function.
   * The transaction is guaranteed to execute exactly once, even if the workflow is retried with the same UUID.
   * If the transaction encounters a Postgres serialization error, retry it.
   * If it encounters any other error, throw it.
   */
  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, ...args: T): Promise<R> {
    const config = this.#operon.transactionConfigMap.get(txn.name);
    if (config === undefined) {
      throw new OperonError(`Unregistered Transaction ${txn.name}`);
    }
    const readOnly = config.readOnly ?? false;
    let retryWaitMillis = 1;
    const backoffFactor = 2;
    const funcId = this.functionIDGetIncrement();
    const span: Span = this.#operon.tracer.startSpan(txn.name, this.span);
    span.setAttributes({
      workflowUUID: this.workflowUUID,
      operationName: txn.name,
      runAs: this.runAs,
      functionID: funcId,
      readOnly: readOnly,
      isolationLevel: config.isolationLevel,
      args: JSON.stringify(args), // TODO enforce skipLogging & request for hashing
    });
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const wrappedTransaction = async (client: UserDatabaseClient): Promise<R> => {
        // Check if this execution previously happened, returning its original result if it did.

        const tCtxt = new TransactionContext(this.#operon.userDatabase.getName(), client, config, this, this.#operon.logger, span, funcId, txn.name);
        const check: R | OperonNull = await this.checkExecution<R>(client, funcId);
        if (check !== operonNull) {
          tCtxt.span.setAttribute("cached", true);
          tCtxt.span.setStatus({ code: SpanStatusCode.OK });
          this.#operon.tracer.endSpan(tCtxt.span);
          return check as R;
        }

        // Flush the result buffer, setting a guard to block concurrent executions with the same UUID.
        this.guardOperation(funcId);
        if (!readOnly) {
          await this.flushResultBuffer(client);
        }

        // Execute the user's transaction.
        const result = await txn(tCtxt, ...args);

        // Record the execution, commit, and return.
        if (readOnly) {
          // Buffer the output of read-only transactions instead of synchronously writing it.
          this.resultBuffer.set(funcId, result);
        } else {
          // Synchronously record the output of write transactions.
          await this.recordGuardedOutput<R>(client, funcId, result);
          this.resultBuffer.clear();
        }

        return result;
      };

      try {
        const result = await this.#operon.userDatabase.transaction(wrappedTransaction, config);
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (err) {
        const errCode = this.#operon.userDatabase.getPostgresErrorCode(err);
        if (errCode === "40001") {
          // serialization_failure in PostgreSQL
          span.addEvent("TXN SERIALIZATION FAILURE", { retryWaitMillis });
          // Retry serialization failures.
          await sleep(retryWaitMillis);
          retryWaitMillis *= backoffFactor;
          continue;
        } else {
          // Record and throw other errors.
          const e: Error = err as Error;
          await this.#operon.userDatabase.transaction(async (client: UserDatabaseClient) => {
            await this.flushResultBuffer(client);
            await this.recordGuardedError(client, funcId, e);
            this.resultBuffer.clear();
          }, {});
          span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
          throw err;
        }
      } finally {
        this.#operon.tracer.endSpan(span);
      }
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
      throw new OperonError(`Unregistered External ${commFn.name}`);
    }

    const funcID = this.functionIDGetIncrement();

    const span: Span = this.#operon.tracer.startSpan(commFn.name, this.span);
    span.setAttributes({
      workflowUUID: this.workflowUUID,
      operationName: commFn.name,
      runAs: this.runAs,
      functionID: funcID,
      retriesAllowed: commConfig.retriesAllowed,
      intervalSeconds: commConfig.intervalSeconds,
      maxAttempts: commConfig.maxAttempts,
      backoffRate: commConfig.backoffRate,
      args: JSON.stringify(args), // TODO enforce skipLogging & request for hashing
    });
    const ctxt: CommunicatorContext = new CommunicatorContext(funcID, span, commConfig);

    await this.#operon.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
      this.resultBuffer.clear();
    }, {});

    // Check if this execution previously happened, returning its original result if it did.
    const check: R | OperonNull = await this.#operon.systemDatabase.checkCommunicatorOutput<R>(this.workflowUUID, ctxt.functionID);
    if (check !== operonNull) {
      ctxt.span.setAttribute("cached", true);
      ctxt.span.setStatus({ code: SpanStatusCode.OK });
      this.#operon.tracer.endSpan(ctxt.span);
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
          ctxt.span.setStatus({ code: SpanStatusCode.ERROR, message: (error as Error).message });
          this.#operon.tracer.endSpan(ctxt.span);
        }
      }
    } else {
      try {
        result = await commFn(ctxt, ...args);
      } catch (error) {
        err = error as Error;
        ctxt.span.setStatus({ code: SpanStatusCode.ERROR, message: (error as Error).message });
        this.#operon.tracer.endSpan(ctxt.span);
      }
    }

    // `result` can only be operonNull when the communicator timed out
    if (result === operonNull) {
      // Record the error, then throw it.
      err = err === operonNull ? new OperonError("Communicator reached maximum retries.", 1) : err;
      await this.#operon.systemDatabase.recordCommunicatorError(this.workflowUUID, ctxt.functionID, err as Error);
      ctxt.span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      this.#operon.tracer.endSpan(ctxt.span);
      throw err;
    } else {
      // Record the execution and return.
      await this.#operon.systemDatabase.recordCommunicatorOutput<R>(this.workflowUUID, ctxt.functionID, result as R);
      ctxt.span.setStatus({ code: SpanStatusCode.OK });
      this.#operon.tracer.endSpan(ctxt.span);
      return result as R;
    }
  }

  /**
   * Send a message to a key, returning true if successful.
   * If a message is already associated with the key, do nothing and return false.
   */
  async send<T extends NonNullable<any>>(topic: string, key: string, message: T): Promise<boolean> {
    const functionID: number = this.functionIDGetIncrement();

    // Is this receiver permitted to read from this topic?
    const hasTopicPermissions: boolean = this.hasTopicPermissions(topic);
    if (!hasTopicPermissions) {
      throw new OperonTopicPermissionDeniedError(topic, this.workflowUUID, functionID, this.runAs);
    }

    await this.#operon.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
      this.resultBuffer.clear();
    }, {});

    return this.#operon.systemDatabase.send(this.workflowUUID, functionID, topic, key, message);
  }

  /**
   * Receive and consume a message from a key, returning the message.
   * Waits until the message arrives or a timeout is reached.
   * If the timeout is reached, return null.
   */
  async recv<T extends NonNullable<any>>(topic: string, key: string, timeoutSeconds: number = defaultRecvTimeoutSec): Promise<T | null> {
    const functionID: number = this.functionIDGetIncrement();

    // Is this receiver permitted to read from this topic?
    const hasTopicPermissions: boolean = this.hasTopicPermissions(topic);
    if (!hasTopicPermissions) {
      throw new OperonTopicPermissionDeniedError(topic, this.workflowUUID, functionID, this.runAs);
    }

    await this.#operon.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
      this.resultBuffer.clear();
    }, {});

    return this.#operon.systemDatabase.recv(this.workflowUUID, functionID, topic, key, timeoutSeconds);
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
  getStatus(): Promise<WorkflowStatus>;
  getResult(): Promise<R>;
  getWorkflowUUID(): string;
}

/**
 * The handle returned when invoking a workflow with Operon.workflow
 */
export class InvokedHandle<R> implements WorkflowHandle<R> {
  constructor(readonly systemDatabase: SystemDatabase, readonly workflowPromise: Promise<R>, readonly workflowUUID: string, readonly workflowName: string) {}

  getWorkflowUUID(): string {
    return this.workflowUUID;
  }

  async getStatus(): Promise<WorkflowStatus> {
    const status = await this.systemDatabase.getWorkflowStatus(this.workflowUUID);
    if (status.status === StatusString.UNKNOWN) {
      return { status: StatusString.UNKNOWN, updatedAtEpochMs: Date.now() };
    } else {
      return status;
    }
  }

  async getResult(): Promise<R> {
    return this.workflowPromise;
  }
}

/**
 * The handle returned when retrieving a workflow with Operon.retrieve
 */
export class RetrievedHandle<R> implements WorkflowHandle<R> {
  constructor(readonly systemDatabase: SystemDatabase, readonly workflowUUID: string) {}

  static readonly pollingIntervalMs: number = 1000;

  getWorkflowUUID(): string {
    return this.workflowUUID;
  }

  async getStatus(): Promise<WorkflowStatus> {
    return await this.systemDatabase.getWorkflowStatus(this.workflowUUID);
  }

  async getResult(): Promise<R> {
    return await this.systemDatabase.getWorkflowResult<R>(this.workflowUUID);
  }
}
