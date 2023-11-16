/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon, OperonNull, operonNull } from "./operon";
import { transaction_outputs } from "../schemas/user_db_schema";
import { IsolationLevel, OperonTransaction, TransactionContext, TransactionContextImpl } from "./transaction";
import { OperonCommunicator, CommunicatorContext, CommunicatorContextImpl } from "./communicator";
import { OperonError, OperonNotRegisteredError, OperonWorkflowConflictUUIDError } from "./error";
import { serializeError, deserializeError } from "serialize-error";
import { sleep } from "./utils";
import { SystemDatabase } from "./system_database";
import { UserDatabaseClient } from "./user_database";
import { SpanStatusCode } from "@opentelemetry/api";
import { Span } from "@opentelemetry/sdk-trace-base";
import { HTTPRequest, OperonContext, OperonContextImpl } from './context';
import { getRegisteredOperations } from "./decorators";

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

// Utility type that removes the initial parameter of a function
export type TailParameters<T extends (arg: any, args: any[]) => any> = T extends (arg: any, ...args: infer P) => any ? P : never;

// local type declarations for Operon transaction and communicator functions
type TxFunc = (ctxt: TransactionContext<any>, ...args: any[]) => Promise<any>;
type CommFunc = (ctxt: CommunicatorContext, ...args: any[]) => Promise<any>;

// Utility type that only includes operon transaction/communicator functions + converts the method signature to exclude the context parameter
export type WFInvokeFuncs<T> = {
  [P in keyof T as T[P] extends TxFunc | CommFunc ? P : never]: T[P] extends  TxFunc | CommFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
}

export interface WorkflowParams {
  workflowUUID?: string;
  parentCtx?: OperonContextImpl;
}

export interface WorkflowConfig {
  // TODO: add workflow config here.
}

export interface WorkflowStatus {
  readonly status: string; // The status of the workflow.  One of PENDING, SUCCESS, or ERROR.
  readonly workflowName: string; // The name of the workflow function.
  readonly authenticatedUser: string; // The user who ran the workflow. Empty string if not set.
  readonly assumedRole: string; // The role used to run this workflow.  Empty string if authorization is not required.
  readonly authenticatedRoles: string[]; // All roles the authenticated user has, if any.
  readonly request: HTTPRequest; // The parent request for this workflow, if any.
}

export interface PgTransactionId {
  txid: string;
}

interface BufferedResult {
  output: unknown;
  txn_snapshot: string;
}

export const StatusString = {
  PENDING: "PENDING",
  SUCCESS: "SUCCESS",
  ERROR: "ERROR",
} as const;

export interface WorkflowContext extends OperonContext {
  invoke<T extends object>(targetClass: T): WFInvokeFuncs<T>;
  childWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, ...args: T): Promise<WorkflowHandle<R>>;

  send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string): Promise<void>;
  recv<T extends NonNullable<any>>(topic?: string, timeoutSeconds?: number): Promise<T | null>;
  setEvent<T extends NonNullable<any>>(key: string, value: T): Promise<void>;

  getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds?: number): Promise<T | null>;
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;
}

export class WorkflowContextImpl extends OperonContextImpl implements WorkflowContext {
  functionID: number = 0;
  readonly #operon;
  readonly resultBuffer: Map<number, BufferedResult> = new Map<number, BufferedResult>();
  readonly isTempWorkflow: boolean;

  constructor(
    operon: Operon,
    parentCtx: OperonContextImpl | undefined,
    workflowUUID: string,
    readonly workflowConfig: WorkflowConfig,
    workflowName: string
  ) {
    const span = operon.tracer.startSpan(
      workflowName,
      {
        workflowUUID: workflowUUID,
        operationName: workflowName,
        runAs: parentCtx?.authenticatedUser ?? "",
      },
      parentCtx?.span,
    );
    super(workflowName, span, operon.logger, parentCtx);
    this.workflowUUID = workflowUUID;
    this.#operon = operon;
    this.isTempWorkflow = operon.tempWorkflowName === workflowName;
    if (operon.config.application) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.applicationConfig = operon.config.application;
    }
  }

  functionIDGetIncrement(): number {
    return this.functionID++;
  }

  /**
   * Check if an operation has already executed in a workflow.
   * If it previously executed succesfully, return its output.
   * If it previously executed and threw an error, throw that error.
   * Otherwise, return OperonNull.
   * Also return the transaction snapshot information of this current transaction.
   */
  async checkExecution<R>(client: UserDatabaseClient, funcID: number): Promise<BufferedResult> {
    // Note: we read the current snapshot, not the recorded one!
    const rows = await this.#operon.userDatabase.queryWithClient<transaction_outputs & { recorded: boolean }>(
      client,
      "(SELECT output, error, pg_current_snapshot()::text as txn_snapshot, true as recorded FROM operon.transaction_outputs WHERE workflow_uuid=$1 AND function_id=$2 UNION ALL SELECT null as output, null as error, pg_current_snapshot()::text as txn_snapshot, false as recorded) ORDER BY recorded",
      this.workflowUUID,
      funcID
    );

    if (rows.length === 0 || rows.length > 2) {
      throw new OperonError("This should never happen. Returned rows: " + rows.toString());
    }

    const res: BufferedResult = {
      output: operonNull,
      txn_snapshot: ""
    }
    // recorded=false row will be first because we used ORDER BY.
    res.txn_snapshot = rows[0].txn_snapshot;
    if (rows.length == 2) {
      if (JSON.parse(rows[1].error) !== null) {
        throw deserializeError(JSON.parse(rows[1].error));
      } else {
        res.output = JSON.parse(rows[1].output) as R;
      }
    }
    return res;
  }

  /**
   * Write all entries in the workflow result buffer to the database.
   * If it encounters a primary key error, this indicates a concurrent execution with the same UUID, so throw an OperonError.
   */
  async flushResultBuffer(client: UserDatabaseClient): Promise<void> {
    const funcIDs = Array.from(this.resultBuffer.keys());
    funcIDs.sort();
    try {
      for (const funcID of funcIDs) {
        // Capture output and also transaction snapshot information.
        // Initially, no txn_id because no queries executed.
        const recorded = this.resultBuffer.get(funcID);
        const output = recorded!.output;
        const txnSnapshot = recorded!.txn_snapshot;
        await this.#operon.userDatabase.queryWithClient(
          client,
          "INSERT INTO operon.transaction_outputs (workflow_uuid, function_id, output, error, txn_id, txn_snapshot) VALUES ($1, $2, $3, $4, null, $5);",
          this.workflowUUID,
          funcID,
          JSON.stringify(output),
          JSON.stringify(null),
          txnSnapshot,
        );
      }
    } catch (error) {
      if (this.#operon.userDatabase.isKeyConflictError(error)) {
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
  guardOperation(funcID: number, txnSnapshot: string) {
    const guardOutput: BufferedResult = {
      output: null,
      txn_snapshot: txnSnapshot,
    }
    this.resultBuffer.set(funcID, guardOutput);
  }

  /**
   * Write a guarded operation's output to the database.
   */
  async recordGuardedOutput<R>(client: UserDatabaseClient, funcID: number, output: R): Promise<string> {
    const serialOutput = JSON.stringify(output);
    const rows = await this.#operon.userDatabase.queryWithClient<transaction_outputs>(client, "UPDATE operon.transaction_outputs SET output=$1, txn_id=(select pg_current_xact_id_if_assigned()::text) WHERE workflow_uuid=$2 AND function_id=$3 RETURNING txn_id;", serialOutput, this.workflowUUID, funcID);
    return rows[0].txn_id;  // Must have a transaction ID because we inserted the guard before.
  }

  /**
   * Record an error in a guarded operation to the database.
   */
  async recordGuardedError(client: UserDatabaseClient, funcID: number, err: Error) {
    const serialErr = JSON.stringify(serializeError(err));
    await this.#operon.userDatabase.queryWithClient(client, "UPDATE operon.transaction_outputs SET error=$1 WHERE workflow_uuid=$2 AND function_id=$3;", serialErr, this.workflowUUID, funcID);
  }

  /**
   * Invoke another workflow as its child workflow and return a workflow handle.
   * The child workflow is guaranteed to be executed exactly once, even if the workflow is retried with the same UUID.
   * We pass in itself as a parent context adn assign the child workflow with a derministic UUID "this.workflowUUID-functionID", which appends a function ID to its own UUID.
   */
  async childWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, ...args: T): Promise<WorkflowHandle<R>> {
    // Note: cannot use invoke for childWorkflow because of potential recursive types on the workflow itself.
    const funcId = this.functionIDGetIncrement();
    const childUUID: string = this.workflowUUID + "-" + funcId;
    return this.#operon.workflow(wf, { parentCtx: this, workflowUUID: childUUID }, ...args);
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
      throw new OperonNotRegisteredError(txn.name);
    }
    const readOnly = config.readOnly ?? false;
    let retryWaitMillis = 1;
    const backoffFactor = 2;
    const funcId = this.functionIDGetIncrement();
    const span: Span = this.#operon.tracer.startSpan(
      txn.name,
      {
        workflowUUID: this.workflowUUID,
        operationName: txn.name,
        runAs: this.authenticatedUser,
        readOnly: readOnly,
        isolationLevel: config.isolationLevel,
      },
      this.span,
    );
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const wrappedTransaction = async (client: UserDatabaseClient): Promise<R> => {
        // Check if this execution previously happened, returning its original result if it did.

        const tCtxt = new TransactionContextImpl(
          this.#operon.userDatabase.getName(), client, this,
          span, this.#operon.logger, funcId, txn.name,
        );
        const check: BufferedResult = await this.checkExecution<R>(client, funcId);
        if (check.output !== operonNull) {
          tCtxt.span.setAttribute("cached", true);
          tCtxt.span.setStatus({ code: SpanStatusCode.OK });
          this.#operon.tracer.endSpan(tCtxt.span);
          return check.output as R;
        }
        // TODO: record snapshot information in result buffer.

        // Flush the result buffer, setting a guard to block concurrent executions with the same UUID.
        this.guardOperation(funcId, check.txn_snapshot);
        if (!readOnly) {
          await this.flushResultBuffer(client);
        }

        // Execute the user's transaction.
        const result = await txn(tCtxt, ...args);

        // Record the execution, commit, and return.
        if (readOnly) {
          // Buffer the output of read-only transactions instead of synchronously writing it.
          const guardOutput: BufferedResult = {
            output: result,
            txn_snapshot: check.txn_snapshot,
          }
          this.resultBuffer.set(funcId, guardOutput);
        } else {
          // Synchronously record the output of write transactions and obtain the transaction ID.
          const pg_txn_id = await this.recordGuardedOutput<R>(client, funcId, result);
          tCtxt.span.setAttribute("transaction_id", pg_txn_id);
          this.resultBuffer.clear();
        }

        return result;
      };

      try {
        const result = await this.#operon.userDatabase.transaction(wrappedTransaction, config);
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (err) {
        if (this.#operon.userDatabase.isRetriableTransactionError(err)) {
          // serialization_failure in PostgreSQL
          span.addEvent("TXN SERIALIZATION FAILURE", { retryWaitMillis });
          // Retry serialization failures.
          await sleep(retryWaitMillis);
          retryWaitMillis *= backoffFactor;
          continue;
        }

        // Record and throw other errors.
        const e: Error = err as Error;
        await this.#operon.userDatabase.transaction(async (client: UserDatabaseClient) => {
          await this.flushResultBuffer(client);
          await this.recordGuardedError(client, funcId, e);
        }, { isolationLevel: IsolationLevel.ReadCommitted });
        this.resultBuffer.clear();
        span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
        throw err;
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
    const commConfig = this.#operon.communicatorConfigMap.get(commFn.name);
    if (commConfig === undefined) {
      throw new OperonNotRegisteredError(commFn.name);
    }

    const funcID = this.functionIDGetIncrement();

    const span: Span = this.#operon.tracer.startSpan(
      commFn.name,
      {
        workflowUUID: this.workflowUUID,
        operationName: commFn.name,
        runAs: this.authenticatedUser,
        retriesAllowed: commConfig.retriesAllowed,
        intervalSeconds: commConfig.intervalSeconds,
        maxAttempts: commConfig.maxAttempts,
        backoffRate: commConfig.backoffRate,
      },
      this.span,
    );
    const ctxt: CommunicatorContextImpl = new CommunicatorContextImpl(this, funcID, span, this.#operon.logger, commConfig, commFn.name);

    await this.#operon.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    this.resultBuffer.clear();

    // Check if this execution previously happened, returning its original result if it did.
    const check: R | OperonNull = await this.#operon.systemDatabase.checkOperationOutput<R>(this.workflowUUID, ctxt.functionID);
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
      await this.#operon.systemDatabase.recordOperationError(this.workflowUUID, ctxt.functionID, err as Error);
      ctxt.span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      this.#operon.tracer.endSpan(ctxt.span);
      throw err;
    } else {
      // Record the execution and return.
      await this.#operon.systemDatabase.recordOperationOutput<R>(this.workflowUUID, ctxt.functionID, result as R);
      ctxt.span.setStatus({ code: SpanStatusCode.OK });
      this.#operon.tracer.endSpan(ctxt.span);
      return result as R;
    }
  }

  /**
   * Send a message to a workflow identified by a UUID.
   * The message can optionally be tagged with a topic.
   */
  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string): Promise<void> {
    const functionID: number = this.functionIDGetIncrement();

    await this.#operon.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    this.resultBuffer.clear();

    await this.#operon.systemDatabase.send(this.workflowUUID, functionID, destinationUUID, message, topic);
  }

  /**
   * Consume and return the oldest unconsumed message sent to your UUID.
   * If a topic is specified, retrieve the oldest message tagged with that topic.
   * Otherwise, retrieve the oldest message with no topic.
   */
  async recv<T extends NonNullable<any>>(topic?: string, timeoutSeconds: number = Operon.defaultNotificationTimeoutSec): Promise<T | null> {
    const functionID: number = this.functionIDGetIncrement();

    await this.#operon.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    this.resultBuffer.clear();

    return this.#operon.systemDatabase.recv(this.workflowUUID, functionID, topic, timeoutSeconds);
  }

  /**
   * Emit a workflow event, represented as a key-value pair.
   * Events are immutable once set.
   */
  async setEvent<T extends NonNullable<any>>(key: string, value: T) {
    const functionID: number = this.functionIDGetIncrement();

    await this.#operon.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    this.resultBuffer.clear();

    await this.#operon.systemDatabase.setEvent(this.workflowUUID, functionID, key, value);
  }

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to use WorkflowContext.Transaction(OpClass.someMethod, param);
   */
  invoke<T extends object>(object: T): WFInvokeFuncs<T> {
    const ops = getRegisteredOperations(object);

    const proxy: any = {};
    for (const op of ops) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      proxy[op.name] = op.txnConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.transaction(op.registeredFunction as OperonTransaction<any[], any>, ...args)
        : op.commConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.external(op.registeredFunction as OperonCommunicator<any[], any>, ...args)
        : undefined;
    }
    return proxy as WFInvokeFuncs<T>;
  }

  /**
   * Wait for a workflow to emit an event, then return its value.
   */
  getEvent<T extends NonNullable<any>>(targetUUID: string, key: string, timeoutSeconds: number = Operon.defaultNotificationTimeoutSec): Promise<T | null> {
    const functionID: number = this.functionIDGetIncrement();
    return this.#operon.systemDatabase.getEvent(targetUUID, key, timeoutSeconds, this.workflowUUID, functionID);
  }

  /**
   * Retrieve a handle for a workflow UUID.
   */
  retrieveWorkflow<R>(targetUUID: string): WorkflowHandle<R> {
    const functionID: number = this.functionIDGetIncrement();
    return new RetrievedHandle(this.#operon.systemDatabase, targetUUID, this.workflowUUID, functionID);
  }

}

/**
 * Object representing an active or completed workflow execution, identified by the workflow UUID.
 * Allows retrieval of information about the workflow.
 */
export interface WorkflowHandle<R> {
  /**
   * Retrieve the workflow's status.
   * Statuses are updated asynchronously.
   */
  getStatus(): Promise<WorkflowStatus | null>;
  /**
   * Await workflow completion and return its result.
   */
  getResult(): Promise<R>;
  /**
   * Return the workflow's UUID.
   */
  getWorkflowUUID(): string;
}

/**
 * The handle returned when invoking a workflow with Operon.workflow
 */
export class InvokedHandle<R> implements WorkflowHandle<R> {
  constructor(readonly systemDatabase: SystemDatabase, readonly workflowPromise: Promise<R>, readonly workflowUUID: string, readonly workflowName: string,
    readonly callerUUID?: string, readonly callerFunctionID?: number) {}

  getWorkflowUUID(): string {
    return this.workflowUUID;
  }

  async getStatus(): Promise<WorkflowStatus | null> {
    return this.systemDatabase.getWorkflowStatus(this.workflowUUID, this.callerUUID, this.callerFunctionID);
  }

  async getResult(): Promise<R> {
    return this.workflowPromise;
  }
}

/**
 * The handle returned when retrieving a workflow with Operon.retrieve
 */
export class RetrievedHandle<R> implements WorkflowHandle<R> {
  constructor(readonly systemDatabase: SystemDatabase, readonly workflowUUID: string, readonly callerUUID?: string, readonly callerFunctionID?: number) {}

  getWorkflowUUID(): string {
    return this.workflowUUID;
  }

  async getStatus(): Promise<WorkflowStatus | null> {
    return await this.systemDatabase.getWorkflowStatus(this.workflowUUID, this.callerUUID, this.callerFunctionID);
  }

  async getResult(): Promise<R> {
    return await this.systemDatabase.getWorkflowResult<R>(this.workflowUUID);
  }
}
