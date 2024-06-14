/* eslint-disable @typescript-eslint/no-explicit-any */
import { DBOSExecutor, DBOSNull, OperationType, dbosNull } from "./dbos-executor";
import { transaction_outputs } from "../schemas/user_db_schema";
import { IsolationLevel, Transaction, TransactionContext, TransactionContextImpl } from "./transaction";
import { Communicator, CommunicatorContext, CommunicatorContextImpl } from "./communicator";
import { DBOSError, DBOSNotRegisteredError, DBOSWorkflowConflictUUIDError } from "./error";
import { serializeError, deserializeError } from "serialize-error";
import { DBOSJSON, sleepms } from "./utils";
import { SystemDatabase } from "./system_database";
import { UserDatabaseClient } from "./user_database";
import { SpanStatusCode } from "@opentelemetry/api";
import { Span } from "@opentelemetry/sdk-trace-base";
import { HTTPRequest, DBOSContext, DBOSContextImpl } from './context';
import { ConfiguredInstance, getRegisteredOperations } from "./decorators";
import { StoredProcedure, StoredProcedureContext } from "./procedure";
import { InvokeFuncsInst } from "./httpServer/handler";

export type Workflow<T extends unknown[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

// Utility type that removes the initial parameter of a function
export type TailParameters<T extends (arg: any, args: any[]) => any> = T extends (arg: any, ...args: infer P) => any ? P : never;

// local type declarations for transaction and communicator functions
type TxFunc = (ctxt: TransactionContext<any>, ...args: any[]) => Promise<any>;
type CommFunc = (ctxt: CommunicatorContext, ...args: any[]) => Promise<any>;
type ProcFunc = (ctxt: StoredProcedureContext, ...args: any[]) => Promise<any>;

// Utility type that only includes transaction/communicator/proc functions + converts the method signature to exclude the context parameter
export type WFInvokeFuncs<T> =
  T extends ConfiguredInstance
  ? never
  : {
    [P in keyof T as T[P] extends TxFunc | CommFunc | ProcFunc ? P : never]: T[P] extends TxFunc | CommFunc | ProcFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
  };

export type WFInvokeFuncsInst<T> =
  T extends ConfiguredInstance
  ? {
    [P in keyof T as T[P] extends TxFunc | CommFunc | ProcFunc ? P : never]: T[P] extends TxFunc | CommFunc | ProcFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
  }
  : never;

export interface WorkflowParams {
  workflowUUID?: string;
  parentCtx?: DBOSContextImpl;
  configuredInstance?: ConfiguredInstance | null;
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface WorkflowConfig {
}

export interface WorkflowStatus {
  readonly status: string; // The status of the workflow.  One of PENDING, SUCCESS, or ERROR.
  readonly workflowName: string; // The name of the workflow function.
  readonly workflowClassName: string; // The class name holding the workflow function.
  readonly workflowConfigName: string; // The name of the configuration, if the class needs configuration
  readonly authenticatedUser: string; // The user who ran the workflow. Empty string if not set.
  readonly assumedRole: string; // The role used to run this workflow.  Empty string if authorization is not required.
  readonly authenticatedRoles: string[]; // All roles the authenticated user has, if any.
  readonly request: HTTPRequest; // The parent request for this workflow, if any.
}

export interface PgTransactionId {
  txid: string;
}

export interface BufferedResult {
  output: unknown;
  txn_snapshot: string;
  created_at?: number;
}

export const StatusString = {
  PENDING: "PENDING",
  SUCCESS: "SUCCESS",
  ERROR: "ERROR",
} as const;

type WFFunc = (ctxt: WorkflowContext, ...args: any[]) => Promise<unknown>;
export type WfInvokeWfs<T> = {
  [P in keyof T]: T[P] extends WFFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
}
export type WfInvokeWfsAsync<T> = {
  [P in keyof T]: T[P] extends WFFunc ? (...args: TailParameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>> : never;
}

export type WfInvokeWfsInst<T> =
  T extends ConfiguredInstance
  ? {
    [P in keyof T]: T[P] extends WFFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
  }
  : never;

export type WfInvokeWfsInstAsync<T> =
  T extends ConfiguredInstance
  ? {
    [P in keyof T]: T[P] extends WFFunc ? (...args: TailParameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>> : never;
  }
  : never;

export interface WorkflowContext extends DBOSContext {
  invoke<T extends ConfiguredInstance>(targetCfg: T): InvokeFuncsInst<T>;
  invoke<T extends object>(targetClass: T): WFInvokeFuncs<T>;

  /** @deprecated See startWorkflow */
  startChildWorkflow<T extends any[], R>(wf: Workflow<T, R>, ...args: T): Promise<WorkflowHandle<R>>;
  /** @deprecated See invokeWorkflow */
  invokeChildWorkflow<T extends unknown[], R>(wf: Workflow<T, R>, ...args: T): Promise<R>;

  /** @deprecated calls startWorkflow */
  childWorkflow<T extends unknown[], R>(wf: Workflow<T, R>, ...args: T): Promise<WorkflowHandle<R>>;

  // These aren't perfectly type checked (return some methods that should not be called) but the syntax is otherwise the neatest
  invokeWorkflow<T extends ConfiguredInstance>(targetClass: T): WfInvokeWfsInst<T>;
  invokeWorkflow<T extends object>(targetClass: T): WfInvokeWfs<T>;
  startWorkflow<T extends ConfiguredInstance>(targetClass: T): WfInvokeWfsInstAsync<T>;
  startWorkflow<T extends object>(targetClass: T): WfInvokeWfsAsync<T>;

  // These are subject to change...

  send<T>(destinationUUID: string, message: T, topic?: string): Promise<void>;
  recv<T>(topic?: string, timeoutSeconds?: number): Promise<T | null>;
  setEvent<T>(key: string, value: T): Promise<void>;
  getEvent<T>(workflowUUID: string, key: string, timeoutSeconds?: number): Promise<T | null>;

  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;

  sleepms(durationMS: number): Promise<void>;
  sleep(durationSec: number): Promise<void>;
}

export class WorkflowContextImpl extends DBOSContextImpl implements WorkflowContext {
  functionID: number = 0;
  readonly #dbosExec;
  readonly resultBuffer: Map<number, BufferedResult> = new Map<number, BufferedResult>();
  readonly isTempWorkflow: boolean;

  constructor(
    dbosExec: DBOSExecutor,
    parentCtx: DBOSContextImpl | undefined,
    workflowUUID: string,
    readonly workflowConfig: WorkflowConfig,
    workflowName: string,
    readonly presetUUID: boolean,
    readonly tempWfOperationType: string = "", // "transaction", "external", or "send"
    readonly tempWfOperationName: string = "" // Name for the temporary workflow operation
  ) {
    const span = dbosExec.tracer.startSpan(
      workflowName,
      {
        status: StatusString.PENDING,
        operationUUID: workflowUUID,
        operationType: OperationType.WORKFLOW,
        authenticatedUser: parentCtx?.authenticatedUser ?? "",
        authenticatedRoles: parentCtx?.authenticatedRoles ?? [],
        assumedRole: parentCtx?.assumedRole ?? "",
        executorID: parentCtx?.executorID,
      },
      parentCtx?.span,
    );
    super(workflowName, span, dbosExec.logger, parentCtx);
    this.workflowUUID = workflowUUID;
    this.#dbosExec = dbosExec;
    this.isTempWorkflow = DBOSExecutor.tempWorkflowName === workflowName;
    this.applicationConfig = dbosExec.config.application;
  }

  functionIDGetIncrement(): number {
    return this.functionID++;
  }

  /**
   * Retrieve the transaction snapshot information of the current transaction
   */
  async retrieveSnapshot(client: UserDatabaseClient): Promise<string> {
    const rows = await this.#dbosExec.userDatabase.queryWithClient<{ txn_snapshot: string }>(client, "SELECT pg_current_snapshot()::text as txn_snapshot;");
    return rows[0].txn_snapshot;
  }

  /**
   * Check if an operation has already executed in a workflow.
   * If it previously executed successfully, return its output.
   * If it previously executed and threw an error, throw that error.
   * Otherwise, return DBOSNull.
   * Also return the transaction snapshot information of this current transaction.
   */
  async checkExecution<R>(client: UserDatabaseClient, funcID: number): Promise<BufferedResult> {
    // Note: we read the current snapshot, not the recorded one!
    const rows = await this.#dbosExec.userDatabase.queryWithClient<transaction_outputs & { recorded: boolean }>(
      client,
      "(SELECT output, error, txn_snapshot, true as recorded FROM dbos.transaction_outputs WHERE workflow_uuid=$1 AND function_id=$2 UNION ALL SELECT null as output, null as error, pg_current_snapshot()::text as txn_snapshot, false as recorded) ORDER BY recorded",
      this.workflowUUID,
      funcID
    );

    if (rows.length === 0 || rows.length > 2) {
      this.logger.error("Unexpected! This should never happen. Returned rows: " + rows.toString());
      throw new DBOSError("This should never happen. Returned rows: " + rows.toString());
    }

    const res: BufferedResult = {
      output: dbosNull,
      txn_snapshot: ""
    }
    // recorded=false row will be first because we used ORDER BY.
    res.txn_snapshot = rows[0].txn_snapshot;
    if (rows.length === 2) {
      if (DBOSJSON.parse(rows[1].error) !== null) {
        throw deserializeError(DBOSJSON.parse(rows[1].error));
      } else {
        res.output = DBOSJSON.parse(rows[1].output) as R;
      }
    }
    return res;
  }

  /**
   * Write all entries in the workflow result buffer to the database.
   * If it encounters a primary key error, this indicates a concurrent execution with the same UUID, so throw an DBOSError.
   */
  async flushResultBuffer(client: UserDatabaseClient): Promise<void> {
    const funcIDs = Array.from(this.resultBuffer.keys());
    if (funcIDs.length === 0) {
      return;
    }
    funcIDs.sort();
    try {
      let sqlStmt = "INSERT INTO dbos.transaction_outputs (workflow_uuid, function_id, output, error, txn_id, txn_snapshot, created_at) VALUES ";
      let paramCnt = 1;
      const values: any[] = [];
      for (const funcID of funcIDs) {
        // Capture output and also transaction snapshot information.
        // Initially, no txn_id because no queries executed.
        const recorded = this.resultBuffer.get(funcID);
        const output = recorded!.output;
        const txnSnapshot = recorded!.txn_snapshot;
        const createdAt = recorded!.created_at!;
        if (paramCnt > 1) {
          sqlStmt += ", ";
        }
        sqlStmt += `($${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, null, $${paramCnt++}, $${paramCnt++})`;
        values.push(this.workflowUUID, funcID, DBOSJSON.stringify(output), DBOSJSON.stringify(null), txnSnapshot, createdAt);
      }
      this.logger.debug(sqlStmt);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      await this.#dbosExec.userDatabase.queryWithClient(client, sqlStmt, ...values);
    } catch (error) {
      if (this.#dbosExec.userDatabase.isKeyConflictError(error)) {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(this.workflowUUID);
      } else {
        throw error;
      }
    }
  }

  /**
   * Write a operation's output to the database.
   */
  async recordOutput<R>(client: UserDatabaseClient, funcID: number, txnSnapshot: string, output: R): Promise<string> {
    try {
      const serialOutput = DBOSJSON.stringify(output);
      const rows = await this.#dbosExec.userDatabase.queryWithClient<transaction_outputs>(client, "INSERT INTO dbos.transaction_outputs (workflow_uuid, function_id, output, txn_id, txn_snapshot, created_at) VALUES ($1, $2, $3, (select pg_current_xact_id_if_assigned()::text), $4, $5) RETURNING txn_id;", this.workflowUUID, funcID, serialOutput, txnSnapshot, Date.now());
      return rows[0].txn_id;
    } catch (error) {
      if (this.#dbosExec.userDatabase.isKeyConflictError(error)) {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(this.workflowUUID);
      } else {
        throw error;
      }
    }
  }

  /**
   * Record an error in an operation to the database.
   */
  async recordError(client: UserDatabaseClient, funcID: number, txnSnapshot: string, err: Error): Promise<void> {
    try {
      const serialErr = DBOSJSON.stringify(serializeError(err));
      await this.#dbosExec.userDatabase.queryWithClient<transaction_outputs>(client, "INSERT INTO dbos.transaction_outputs (workflow_uuid, function_id, error, txn_id, txn_snapshot, created_at) VALUES ($1, $2, $3, null, $4, $5) RETURNING txn_id;", this.workflowUUID, funcID, serialErr, txnSnapshot, Date.now());
    } catch (error) {
      if (this.#dbosExec.userDatabase.isKeyConflictError(error)) {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(this.workflowUUID);
      } else {
        throw error;
      }
    }
  }

  /**
   * Invoke another workflow as its child workflow and return a workflow handle.
   * The child workflow is guaranteed to be executed exactly once, even if the workflow is retried with the same UUID.
   * We pass in itself as a parent context and assign the child workflow with a deterministic UUID "this.workflowUUID-functionID".
   * We also pass in its own workflowUUID and function ID so the invoked handle is deterministic.
   */
  async startChildWorkflow<T extends unknown[], R>(wf: Workflow<T, R>, ...args: T): Promise<WorkflowHandle<R>> {
    // Note: cannot use invoke for childWorkflow because of potential recursive types on the workflow itself.
    const funcId = this.functionIDGetIncrement();
    const childUUID: string = this.workflowUUID + "-" + funcId;
    return this.#dbosExec.internalWorkflow(wf, { parentCtx: this, workflowUUID: childUUID }, this.workflowUUID, funcId, ...args);
  }

  async invokeChildWorkflow<T extends unknown[], R>(wf: Workflow<T, R>, ...args: T): Promise<R> {
    return this.startChildWorkflow(wf, ...args).then((handle) => handle.getResult());
  }

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to use WorkflowContext.Transaction(OpClass.someMethod, param);
   */
  proxyInvokeWF<T extends object>(object: T, workflowUUID: string | undefined, asyncWf: boolean, configuredInstance: ConfiguredInstance | null):
    WfInvokeWfsAsync<T> {
    const ops = getRegisteredOperations(object);
    const proxy: Record<string, unknown> = {};

    const funcId = this.functionIDGetIncrement();
    const childUUID = workflowUUID || (this.workflowUUID + "-" + funcId);

    const params = { workflowUUID: childUUID, parentCtx: this, configuredInstance };


    for (const op of ops) {
      if (asyncWf) {
        proxy[op.name] = op.workflowConfig
          ? (...args: unknown[]) => this.#dbosExec.internalWorkflow((op.registeredFunction as Workflow<unknown[], unknown>), params, this.workflowUUID, funcId, ...args)
          : undefined;
      } else {
        proxy[op.name] = op.workflowConfig
          ? (...args: unknown[]) => this.#dbosExec.internalWorkflow((op.registeredFunction as Workflow<unknown[], unknown>), params, this.workflowUUID, funcId, ...args)
            .then((handle) => handle.getResult())
          : undefined;
      }
    }
    return proxy as WfInvokeWfsAsync<T>;
  }

  startWorkflow<T extends object>(target: T, workflowUUID?: string): WfInvokeWfsAsync<T> {
    if (typeof target === 'function') {
      return this.proxyInvokeWF(target, workflowUUID, true, null) as unknown as WfInvokeWfsAsync<T>;
    }
    else {
      return this.proxyInvokeWF(target, workflowUUID, true, target as ConfiguredInstance) as unknown as WfInvokeWfsAsync<T>;
    }
  }
  invokeWorkflow<T extends object>(target: T, workflowUUID?: string): WfInvokeWfs<T> {
    if (typeof target === 'function') {
      return this.proxyInvokeWF(target, workflowUUID, false, null) as unknown as WfInvokeWfs<T>;
    }
    else {
      return this.proxyInvokeWF(target, workflowUUID, false, target as ConfiguredInstance) as unknown as WfInvokeWfs<T>;
    }
  }

  async childWorkflow<T extends unknown[], R>(wf: Workflow<T, R>, ...args: T): Promise<WorkflowHandle<R>> {
    return this.startChildWorkflow(wf, ...args);
  }

  // TODO: ConfiguredInstance support
  async procedure<R>(proc: StoredProcedure<R>, ...args: unknown[]): Promise<R> {
    const procInfo = this.#dbosExec.getProcedureInfo(proc);
    if (procInfo === undefined) { throw new DBOSNotRegisteredError(proc.name); }
    const readOnly = procInfo.config.readOnly ?? false;
    const funcId = this.functionIDGetIncrement();
    const span: Span = this.#dbosExec.tracer.startSpan(
      proc.name,
      {
        operationUUID: this.workflowUUID,
        operationType: OperationType.PROCEDURE,
        authenticatedUser: this.authenticatedUser,
        assumedRole: this.assumedRole,
        authenticatedRoles: this.authenticatedRoles,
        readOnly: readOnly,
        isolationLevel: procInfo.config.isolationLevel,
        executorID: this.executorID,
      },
      this.span,
    );

    const $jsonCtx = {
      request: this.request,
      authenticatedUser: this.authenticatedUser,
      authenticatedRoles: this.authenticatedRoles,
      assumedRole: this.assumedRole,
    };

    const $args = [this.workflowUUID, funcId, this.presetUUID, $jsonCtx, null, ...args];
    if (!readOnly) {
      // function_id, output, txn_snapshot, created_at
      const bufferedResults = new Array<[number, unknown, string, number?]>();
      for (const [functionID, { output, txn_snapshot, created_at }] of this.resultBuffer.entries()) {
        bufferedResults.push([functionID, output, txn_snapshot, created_at]);
      }
      // sort by function ID
      bufferedResults.sort((a, b) => a[0] - b[0]);
      $args.unshift(bufferedResults.length > 0 ? JSON.stringify(bufferedResults) : null);
    }

    type ReturnValue = { return_value: { output?: R, error?: unknown, txn_id?: string, txn_snapshot?: string, created_at?: number } };
    try {
      const [{ return_value }] = await this.#dbosExec.callProcedure<ReturnValue>(proc, ...$args);

      const { error, output, txn_snapshot, txn_id, created_at } = return_value;

      // buffered results are persisted in r/w stored procs, even if it returns an error
      if (!readOnly) {
        this.resultBuffer.clear();
      }

      // if the stored proc returns an error, deserialize and throw it. 
      // stored proc saves the error in tx_output before returning 
      if (error) {
        throw deserializeError(error);
      }

      // if txn_snapshot is provided, the output needs to be buffered
      if (readOnly && txn_snapshot) {
        this.resultBuffer.set(funcId, {
          output,
          txn_snapshot,
          created_at: created_at ?? Date.now(),
        });
      }

      if (!readOnly) {
        this.resultBuffer.clear();
      }

      if (txn_id) {
        span.setAttribute("pg_txn_id", txn_id);
      }
      span.setStatus({ code: SpanStatusCode.OK });
      return output!; // output will be undefined if tx function returns void
    } catch (e) {
      const { message } = e as { message: string };
      span.setStatus({ code: SpanStatusCode.ERROR, message });
      throw e;
    } finally {
      this.#dbosExec.tracer.endSpan(span);
    }
  }

  /**
   * Execute a transactional function.
   * The transaction is guaranteed to execute exactly once, even if the workflow is retried with the same UUID.
   * If the transaction encounters a Postgres serialization error, retry it.
   * If it encounters any other error, throw it.
   */
  async transaction<T extends unknown[], R>(txn: Transaction<T, R>, clsinst: ConfiguredInstance | null, ...args: T): Promise<R> {
    const txnInfo = this.#dbosExec.getTransactionInfo(txn as Transaction<unknown[], unknown>);
    if (txnInfo === undefined) {
      throw new DBOSNotRegisteredError(txn.name);
    }
    const readOnly = txnInfo.config.readOnly ?? false;
    let retryWaitMillis = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMs = 2000; // Maximum wait 2 seconds.
    const funcId = this.functionIDGetIncrement();
    const span: Span = this.#dbosExec.tracer.startSpan(
      txn.name,
      {
        operationUUID: this.workflowUUID,
        operationType: OperationType.TRANSACTION,
        authenticatedUser: this.authenticatedUser,
        assumedRole: this.assumedRole,
        authenticatedRoles: this.authenticatedRoles,
        readOnly: readOnly,
        isolationLevel: txnInfo.config.isolationLevel,
        executorID: this.executorID,
      },
      this.span,
    );
    // eslint-disable-next-line no-constant-condition
    while (true) {
      let txn_snapshot = "invalid";
      const wrappedTransaction = async (client: UserDatabaseClient): Promise<R> => {
        const tCtxt = new TransactionContextImpl(
          this.#dbosExec.userDatabase.getName(), client, this,
          span, this.#dbosExec.logger, funcId, txn.name);

        // If the UUID is preset, it is possible this execution previously happened. Check, and return its original result if it did.
        // Note: It is possible to retrieve a generated ID from a workflow handle, run a concurrent execution, and cause trouble for yourself. We recommend against this.
        if (this.presetUUID) {
          const check: BufferedResult = await this.checkExecution<R>(client, funcId);
          txn_snapshot = check.txn_snapshot;
          if (check.output !== dbosNull) {
            tCtxt.span.setAttribute("cached", true);
            tCtxt.span.setStatus({ code: SpanStatusCode.OK });
            this.#dbosExec.tracer.endSpan(tCtxt.span);
            return check.output as R;
          }
        } else {
          // Collect snapshot information for read-only transactions and non-preset UUID transactions, if not already collected above
          txn_snapshot = await this.retrieveSnapshot(client);
        }

        // For non-read-only transactions, flush the result buffer.
        if (!readOnly) {
          await this.flushResultBuffer(client);
        }

        // Execute the user's transaction.
        const result = await txn.call(clsinst, tCtxt, ...args);

        // Record the execution, commit, and return.
        if (readOnly) {
          // Buffer the output of read-only transactions instead of synchronously writing it.
          const readOutput: BufferedResult = {
            output: result,
            txn_snapshot: txn_snapshot,
            created_at: Date.now(),
          }
          this.resultBuffer.set(funcId, readOutput);
        } else {
          // Synchronously record the output of write transactions and obtain the transaction ID.
          const pg_txn_id = await this.recordOutput<R>(client, funcId, txn_snapshot, result);
          tCtxt.span.setAttribute("pg_txn_id", pg_txn_id);
          this.resultBuffer.clear();
        }

        return result;
      };

      try {
        const result = await this.#dbosExec.userDatabase.transaction(wrappedTransaction, txnInfo.config);
        span.setStatus({ code: SpanStatusCode.OK });
        this.#dbosExec.tracer.endSpan(span);
        return result;
      } catch (err) {
        if (this.#dbosExec.userDatabase.isRetriableTransactionError(err)) {
          // serialization_failure in PostgreSQL
          span.addEvent("TXN SERIALIZATION FAILURE", { "retryWaitMillis": retryWaitMillis }, performance.now());
          // Retry serialization failures.
          await sleepms(retryWaitMillis);
          retryWaitMillis *= backoffFactor;
          retryWaitMillis = retryWaitMillis < maxRetryWaitMs ? retryWaitMillis : maxRetryWaitMs;
          continue;
        }

        // Record and throw other errors.
        const e: Error = err as Error;
        await this.#dbosExec.userDatabase.transaction(async (client: UserDatabaseClient) => {
          await this.flushResultBuffer(client);
          await this.recordError(client, funcId, txn_snapshot, e);
        }, { isolationLevel: IsolationLevel.ReadCommitted });
        this.resultBuffer.clear();
        span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
        this.#dbosExec.tracer.endSpan(span);
        throw err;
      }
    }
  }

  /**
   * Execute a communicator function.
   * If it encounters any error, retry according to its configured retry policy until the maximum number of attempts is reached, then throw an DBOSError.
   * The communicator may execute many times, but once it is complete, it will not re-execute.
   */
  async external<T extends unknown[], R>(commFn: Communicator<T, R>, clsInst: ConfiguredInstance | null, ...args: T): Promise<R> {
    const commInfo = this.#dbosExec.getCommunicatorInfo(commFn as Communicator<unknown[], unknown>);
    if (commInfo === undefined) {
      throw new DBOSNotRegisteredError(commFn.name);
    }

    const funcID = this.functionIDGetIncrement();
    const maxRetryIntervalSec = 3600 // Maximum retry interval: 1 hour

    const span: Span = this.#dbosExec.tracer.startSpan(
      commFn.name,
      {
        operationUUID: this.workflowUUID,
        operationType: OperationType.COMMUNICATOR,
        authenticatedUser: this.authenticatedUser,
        assumedRole: this.assumedRole,
        authenticatedRoles: this.authenticatedRoles,
        executorID: this.executorID,
        retriesAllowed: commInfo.config.retriesAllowed,
        intervalSeconds: commInfo.config.intervalSeconds,
        maxAttempts: commInfo.config.maxAttempts,
        backoffRate: commInfo.config.backoffRate,
      },
      this.span,
    );

    const ctxt: CommunicatorContextImpl = new CommunicatorContextImpl(this, funcID, span, this.#dbosExec.logger, commInfo.config, commFn.name);

    await this.#dbosExec.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    this.resultBuffer.clear();

    // Check if this execution previously happened, returning its original result if it did.
    const check: R | DBOSNull = await this.#dbosExec.systemDatabase.checkOperationOutput<R>(this.workflowUUID, ctxt.functionID);
    if (check !== dbosNull) {
      ctxt.span.setAttribute("cached", true);
      ctxt.span.setStatus({ code: SpanStatusCode.OK });
      this.#dbosExec.tracer.endSpan(ctxt.span);
      return check as R;
    }

    // Execute the communicator function.  If it throws an exception, retry with exponential backoff.
    // After reaching the maximum number of retries, throw an DBOSError.
    let result: R | DBOSNull = dbosNull;
    let err: Error | DBOSNull = dbosNull;
    if (ctxt.retriesAllowed) {
      let numAttempts = 0;
      let intervalSeconds: number = ctxt.intervalSeconds;
      if (intervalSeconds > maxRetryIntervalSec) {
        this.logger.warn(`Communicator config interval exceeds maximum allowed interval, capped to ${maxRetryIntervalSec} seconds!`)
      }
      while (result === dbosNull && numAttempts++ < ctxt.maxAttempts) {
        try {
          result = await commFn.call(clsInst, ctxt, ...args);
        } catch (error) {
          this.logger.error(error);
          span.addEvent(`Communicator attempt ${numAttempts + 1} failed`, { "retryIntervalSeconds": intervalSeconds, "error": (error as Error).message }, performance.now());
          if (numAttempts < ctxt.maxAttempts) {
            // Sleep for an interval, then increase the interval by backoffRate.
            // Cap at the maximum allowed retry interval.
            await sleepms(intervalSeconds * 1000);
            intervalSeconds *= ctxt.backoffRate;
            intervalSeconds = intervalSeconds < maxRetryIntervalSec ? intervalSeconds : maxRetryIntervalSec;
          }
        }
      }
    } else {
      try {
        result = await commFn.call(clsInst, ctxt, ...args);
      } catch (error) {
        err = error as Error;
      }
    }

    // `result` can only be dbosNull when the communicator timed out
    if (result === dbosNull) {
      // Record the error, then throw it.
      err = err === dbosNull ? new DBOSError("Communicator reached maximum retries.", 1) : err;
      await this.#dbosExec.systemDatabase.recordOperationError(this.workflowUUID, ctxt.functionID, err as Error);
      ctxt.span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      this.#dbosExec.tracer.endSpan(ctxt.span);
      throw err as Error;
    } else {
      // Record the execution and return.
      await this.#dbosExec.systemDatabase.recordOperationOutput<R>(this.workflowUUID, ctxt.functionID, result as R);
      ctxt.span.setStatus({ code: SpanStatusCode.OK });
      this.#dbosExec.tracer.endSpan(ctxt.span);
      return result as R;
    }
  }

  /**
   * Send a message to a workflow identified by a UUID.
   * The message can optionally be tagged with a topic.
   */
  async send<T>(destinationUUID: string, message: T, topic?: string): Promise<void> {
    const functionID: number = this.functionIDGetIncrement();

    await this.#dbosExec.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    this.resultBuffer.clear();

    await this.#dbosExec.systemDatabase.send(this.workflowUUID, functionID, destinationUUID, message, topic);
  }

  /**
   * Consume and return the oldest unconsumed message sent to your UUID.
   * If a topic is specified, retrieve the oldest message tagged with that topic.
   * Otherwise, retrieve the oldest message with no topic.
   */
  async recv<T>(topic?: string, timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec): Promise<T | null> {
    const functionID: number = this.functionIDGetIncrement();

    await this.#dbosExec.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    this.resultBuffer.clear();

    return this.#dbosExec.systemDatabase.recv(this.workflowUUID, functionID, topic, timeoutSeconds);
  }

  /**
   * Emit a workflow event, represented as a key-value pair.
   * Events are immutable once set.
   */
  async setEvent<T>(key: string, value: T) {
    const functionID: number = this.functionIDGetIncrement();

    await this.#dbosExec.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    this.resultBuffer.clear();

    await this.#dbosExec.systemDatabase.setEvent(this.workflowUUID, functionID, key, value);
  }

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to use WorkflowContext.Transaction(OpClass.someMethod, param);
   */
  invoke<T extends object>(object: T | ConfiguredInstance): WFInvokeFuncs<T> | InvokeFuncsInst<T> {
    if (typeof object === 'function') {
      const ops = getRegisteredOperations(object);

      const proxy: Record<string, unknown> = {};
      for (const op of ops) {
        proxy[op.name] = op.txnConfig
          ? (...args: unknown[]) => this.transaction(op.registeredFunction as Transaction<unknown[], unknown>, null, ...args)
          : op.commConfig
            ? (...args: unknown[]) => this.external(op.registeredFunction as Communicator<unknown[], unknown>, null, ...args)
            : op.procConfig
              ? (...args: unknown[]) => this.procedure(op.registeredFunction as StoredProcedure<unknown>, ...args)
            : undefined;
      }
      return proxy as WFInvokeFuncs<T>;
    }
    else {
      const targetInst = object as ConfiguredInstance;
      const ops = getRegisteredOperations(targetInst);

      const proxy: Record<string, unknown> = {};
      for (const op of ops) {
        proxy[op.name] = op.txnConfig
          ? (...args: unknown[]) => this.transaction(op.registeredFunction as Transaction<unknown[], unknown>, targetInst, ...args)
          : op.commConfig
            ? (...args: unknown[]) => this.external(op.registeredFunction as Communicator<unknown[], unknown>, targetInst, ...args)
              : undefined;
      }
      return proxy as InvokeFuncsInst<T>;
    }
  }

  /**
   * Wait for a workflow to emit an event, then return its value.
   */
  getEvent<T>(targetUUID: string, key: string, timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec): Promise<T | null> {
    const functionID: number = this.functionIDGetIncrement();
    return this.#dbosExec.systemDatabase.getEvent(targetUUID, key, timeoutSeconds, this.workflowUUID, functionID);
  }

  /**
   * Retrieve a handle for a workflow UUID.
   */
  retrieveWorkflow<R>(targetUUID: string): WorkflowHandle<R> {
    const functionID: number = this.functionIDGetIncrement();
    return new RetrievedHandle(this.#dbosExec.systemDatabase, targetUUID, this.workflowUUID, functionID);
  }

  /**
   * Sleep for the duration.
   */
  async sleepms(durationMS: number): Promise<void> {
    if (durationMS <= 0) {
      return;
    }
    const functionID = this.functionIDGetIncrement()
    return await this.#dbosExec.systemDatabase.sleepms(this.workflowUUID, functionID, durationMS);
  }

  async sleep(durationSec: number): Promise<void> {
    return this.sleepms(durationSec * 1000);
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
 * The handle returned when invoking a workflow with DBOSExecutor.workflow
 */
export class InvokedHandle<R> implements WorkflowHandle<R> {
  constructor(readonly systemDatabase: SystemDatabase, readonly workflowPromise: Promise<R>, readonly workflowUUID: string, readonly workflowName: string,
    readonly callerUUID?: string, readonly callerFunctionID?: number) { }

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
 * The handle returned when retrieving a workflow with DBOSExecutor.retrieve
 */
export class RetrievedHandle<R> implements WorkflowHandle<R> {
  constructor(readonly systemDatabase: SystemDatabase, readonly workflowUUID: string, readonly callerUUID?: string, readonly callerFunctionID?: number) { }

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
