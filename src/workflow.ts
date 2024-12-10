/* eslint-disable @typescript-eslint/no-explicit-any */
import { DBOSExecutor, OperationType, dbosNull } from "./dbos-executor";
import { transaction_outputs } from "../schemas/user_db_schema";
import { IsolationLevel, Transaction, TransactionContext } from "./transaction";
import { StepFunction, StepContext } from "./step";
import { DBOSError, DBOSNotRegisteredError, DBOSWorkflowConflictUUIDError } from "./error";
import { serializeError, deserializeError } from "serialize-error";
import { DBOSJSON, sleepms } from "./utils";
import { SystemDatabase } from "./system_database";
import { UserDatabaseClient, pgNodeIsKeyConflictError } from "./user_database";
import { SpanStatusCode } from "@opentelemetry/api";
import { Span } from "@opentelemetry/sdk-trace-base";
import { HTTPRequest, DBOSContext, DBOSContextImpl, runWithDBOSContext } from './context';
import { ConfiguredInstance, getRegisteredOperations } from "./decorators";
import { StoredProcedure, StoredProcedureConfig, StoredProcedureContext, StoredProcedureContextImpl } from "./procedure";
import { InvokeFuncsInst } from "./httpServer/handler";
import { PoolClient } from "pg";
import { WorkflowQueue } from "./wfqueue";

export type Workflow<T extends unknown[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;
export type WorkflowFunction<T extends unknown[], R> = Workflow<T, R>;
export type ContextFreeFunction<T extends unknown[], R> = (...args: T) => Promise<R>;

// Utility type that removes the initial parameter of a function
export type TailParameters<T extends (arg: any, args: any[]) => any> = T extends (arg: any, ...args: infer P) => any ? P : never;

// local type declarations for transaction and step functions
type TxFunc = (ctxt: TransactionContext<any>, ...args: any[]) => Promise<any>;
type StepFunc = (ctxt: StepContext, ...args: any[]) => Promise<any>;
type ProcFunc = (ctxt: StoredProcedureContext, ...args: any[]) => Promise<any>;

// Utility type that only includes transaction/step/proc functions + converts the method signature to exclude the context parameter
export type WFInvokeFuncs<T> =
  T extends ConfiguredInstance
  ? never
  : {
    [P in keyof T as T[P] extends TxFunc | StepFunc | ProcFunc ? P : never]: T[P] extends TxFunc | StepFunc | ProcFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
  };

export type WFInvokeFuncsInst<T> =
  T extends ConfiguredInstance
  ? {
    [P in keyof T as T[P] extends TxFunc | StepFunc | ProcFunc ? P : never]: T[P] extends TxFunc | StepFunc | ProcFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
  }
  : never;

export interface WorkflowParams {
  workflowUUID?: string;
  parentCtx?: DBOSContextImpl;
  configuredInstance?: ConfiguredInstance | null;
  recovery?: boolean;
  queueName?: string;
  executeWorkflow?: boolean; // If queueName is set, this will not be run unless executeWorkflow is true.
}

export interface WorkflowConfig {
  maxRecoveryAttempts?: number;
}

export interface WorkflowStatus {
  readonly status: string; // The status of the workflow.  One of PENDING, SUCCESS, ERROR, RETRIES_EXCEEDED, ENQUEUED, or CANCELLED.
  readonly workflowName: string; // The name of the workflow function.
  readonly workflowClassName: string; // The class name holding the workflow function.
  readonly workflowConfigName: string; // The name of the configuration, if the class needs configuration
  readonly queueName?: string; // The name of the queue, if workflow was queued
  readonly authenticatedUser: string; // The user who ran the workflow. Empty string if not set.
  readonly assumedRole: string; // The role used to run this workflow.  Empty string if authorization is not required.
  readonly authenticatedRoles: string[]; // All roles the authenticated user has, if any.
  readonly request: HTTPRequest; // The parent request for this workflow, if any.
}

export interface GetWorkflowsInput {
  workflowName?: string; // The name of the workflow function
  authenticatedUser?: string; // The user who ran the workflow.
  startTime?: string; // Timestamp in ISO 8601 format
  endTime?: string; // Timestamp in ISO 8601 format
  status?: "PENDING" | "SUCCESS" | "ERROR" | "RETRIES_EXCEEDED" | "CANCELLED" | "ENQUEUED"; // The status of the workflow.
  applicationVersion?: string; // The application version that ran this workflow.
  limit?: number; // Return up to this many workflows IDs. IDs are ordered by workflow creation time.
}

export interface GetWorkflowsOutput {
  workflowUUIDs: string[];
}

export interface GetWorkflowQueueInput {
  queueName?: string; // The name of the workflow queue
  startTime?: string; // Timestamp in ISO 8601 format
  endTime?: string; // Timestamp in ISO 8601 format
  limit?: number; // Return up to this many workflows IDs. IDs are ordered by workflow creation time.
}

export interface GetWorkflowQueueOutput {
  workflows: {
    workflowID: string; // Workflow ID
    queueName: string; // Workflow queue name
    createdAt: number; // Time that queue entry was created
    startedAt?: number; // Time that workflow was started, if started
    completedAt?: number; // Time that workflow completed, if complete
  }[];
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
  RETRIES_EXCEEDED: "RETRIES_EXCEEDED",
  CANCELLED: "CANCELLED",
  ENQUEUED: "ENQUEUED",
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
  invokeWorkflow<T extends ConfiguredInstance>(targetClass: T, workflowUUID?: string): WfInvokeWfsInst<T>;
  invokeWorkflow<T extends object>(targetClass: T, workflowUUID?: string): WfInvokeWfs<T>;
  startWorkflow<T extends ConfiguredInstance>(targetClass: T, workflowUUID?: string, queue?: WorkflowQueue): WfInvokeWfsInstAsync<T>;
  startWorkflow<T extends object>(targetClass: T, workflowUUID?: string, queue?: WorkflowQueue): WfInvokeWfsAsync<T>;

  // These are subject to change...

  send<T>(destinationID: string, message: T, topic?: string): Promise<void>;
  recv<T>(topic?: string, timeoutSeconds?: number): Promise<T | null>;
  setEvent<T>(key: string, value: T): Promise<void>;
  getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null>;

  retrieveWorkflow<R>(workflowID: string): WorkflowHandle<R>;

  sleepms(durationMS: number): Promise<void>;
  sleep(durationSec: number): Promise<void>;
}

type QueryFunction = <T>(sql: string, args: unknown[]) => Promise<T[]>;

export class WorkflowContextImpl extends DBOSContextImpl implements WorkflowContext {
  functionID: number = 0;
  readonly #dbosExec;
  readonly resultBuffer: Map<number, BufferedResult> = new Map<number, BufferedResult>();
  readonly isTempWorkflow: boolean;
  readonly maxRecoveryAttempts;

  constructor(
    dbosExec: DBOSExecutor,
    parentCtx: DBOSContextImpl | undefined,
    workflowUUID: string,
    readonly workflowConfig: WorkflowConfig,
    workflowName: string,
    readonly presetUUID: boolean,
    readonly tempWfOperationType: string = "", // "transaction", "procedure", "external", or "send"
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
      },
      parentCtx?.span,
    );
    super(workflowName, span, dbosExec.logger, parentCtx);
    this.workflowUUID = workflowUUID;
    this.#dbosExec = dbosExec;
    this.isTempWorkflow = DBOSExecutor.tempWorkflowName === workflowName;
    this.applicationConfig = dbosExec.config.application;
    this.maxRecoveryAttempts = workflowConfig.maxRecoveryAttempts ? workflowConfig.maxRecoveryAttempts : 50;
  }

  functionIDGetIncrement(): number {
    return this.functionID++;
  }

  /**
   * Write all entries in the workflow result buffer to the database.
   * If it encounters a primary key error, this indicates a concurrent execution with the same UUID, so throw an DBOSError.
   */
  async #flushResultBuffer(query: QueryFunction, isKeyConflict: (error: unknown) => boolean): Promise<void> {
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
      await query(sqlStmt, values);
    } catch (error) {
      if (isKeyConflict(error)) {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(this.workflowUUID);
      } else {
        throw error;
      }
    }
  }

  flushResultBuffer(client: UserDatabaseClient): Promise<void> {
    const func = <T>(sql: string, args: unknown[]) => this.#dbosExec.userDatabase.queryWithClient<T>(client, sql, ...args);
    return this.#flushResultBuffer(func, (error) => this.#dbosExec.userDatabase.isKeyConflictError(error));
  }

  flushResultBufferProc(client: PoolClient): Promise<void> {
    const func = <T>(sql: string, args: unknown[]) => client.query(sql, args).then(v => v.rows as T[]);
    return this.#flushResultBuffer(func, pgNodeIsKeyConflictError);
  }


  /**
   * Write a operation's output to the database.
   */
  async #recordOutput<R>(query: QueryFunction, funcID: number, txnSnapshot: string, output: R, isKeyConflict: (error: unknown) => boolean): Promise<string> {
    try {
      const serialOutput = DBOSJSON.stringify(output);
      const rows = await query<transaction_outputs>("INSERT INTO dbos.transaction_outputs (workflow_uuid, function_id, output, txn_id, txn_snapshot, created_at) VALUES ($1, $2, $3, (select pg_current_xact_id_if_assigned()::text), $4, $5) RETURNING txn_id;",
        [this.workflowUUID, funcID, serialOutput, txnSnapshot, Date.now()]);
      return rows[0].txn_id;
    } catch (error) {
      if (isKeyConflict(error)) {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(this.workflowUUID);
      } else {
        throw error;
      }
    }
  }

  recordOutputTx<R>(client: UserDatabaseClient, funcID: number, txnSnapshot: string, output: R): Promise<string> {
    const func = <T>(sql: string, args: unknown[]) => this.#dbosExec.userDatabase.queryWithClient<T>(client, sql, ...args);
    return this.#recordOutput(func, funcID, txnSnapshot, output, (error) => this.#dbosExec.userDatabase.isKeyConflictError(error));
  }

  recordOutputProc<R>(client: PoolClient, funcID: number, txnSnapshot: string, output: R): Promise<string> {
    const func = <T>(sql: string, args: unknown[]) => client.query(sql, args).then(v => v.rows as T[]);
    return this.#recordOutput(func, funcID, txnSnapshot, output, pgNodeIsKeyConflictError);
  }

  /**
   * Record an error in an operation to the database.
   */
  async #recordError(query: QueryFunction, funcID: number, txnSnapshot: string, err: Error, isKeyConflict: (error: unknown) => boolean): Promise<void> {
    try {
      const serialErr = DBOSJSON.stringify(serializeError(err));
      await query<transaction_outputs>("INSERT INTO dbos.transaction_outputs (workflow_uuid, function_id, error, txn_id, txn_snapshot, created_at) VALUES ($1, $2, $3, null, $4, $5) RETURNING txn_id;",
        [this.workflowUUID, funcID, serialErr, txnSnapshot, Date.now()]);
    } catch (error) {
      if (isKeyConflict(error)) {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(this.workflowUUID);
      } else {
        throw error;
      }
    }
  }

  recordErrorTx(client: UserDatabaseClient, funcID: number, txnSnapshot: string, err: Error): Promise<void> {
    const func = <T>(sql: string, args: unknown[]) => this.#dbosExec.userDatabase.queryWithClient<T>(client, sql, ...args);
    return this.#recordError(func, funcID, txnSnapshot, err, (error) => this.#dbosExec.userDatabase.isKeyConflictError(error));
  }

  recordErrorProc(client: PoolClient, funcID: number, txnSnapshot: string, err: Error): Promise<void> {
    const func = <T>(sql: string, args: unknown[]) => client.query(sql, args).then(v => v.rows as T[]);
    return this.#recordError(func, funcID, txnSnapshot, err, pgNodeIsKeyConflictError);
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
    return this.#dbosExec.internalWorkflow(wf, {
      parentCtx: this, workflowUUID: childUUID,
    }, this.workflowUUID, funcId, ...args);
  }

  async invokeChildWorkflow<T extends unknown[], R>(wf: Workflow<T, R>, ...args: T): Promise<R> {
    return this.startChildWorkflow(wf, ...args).then((handle) => handle.getResult());
  }

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to use WorkflowContext.Transaction(OpClass.someMethod, param);
   */
  proxyInvokeWF<T extends object>(object: T, workflowUUID: string | undefined, asyncWf: boolean, configuredInstance: ConfiguredInstance | null, queue?: WorkflowQueue):
    WfInvokeWfsAsync<T> {
    const ops = getRegisteredOperations(object);
    const proxy: Record<string, unknown> = {};

    const funcId = this.functionIDGetIncrement();
    const childUUID = workflowUUID || (this.workflowUUID + "-" + funcId);

    const params = { workflowUUID: childUUID, parentCtx: this, configuredInstance, queueName: queue?.name };

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

  startWorkflow<T extends object>(target: T, workflowUUID?: string, queue?: WorkflowQueue): WfInvokeWfsAsync<T> {
    if (typeof target === 'function') {
      return this.proxyInvokeWF(target, workflowUUID, true, null, queue) as unknown as WfInvokeWfsAsync<T>;
    }
    else {
      return this.proxyInvokeWF(target, workflowUUID, true, target as ConfiguredInstance, queue) as unknown as WfInvokeWfsAsync<T>;
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
  async procedure<T extends unknown[], R>(proc: StoredProcedure<T, R>, ...args: T): Promise<R> {
    return this.#dbosExec.callProcedureFunction(proc, this, ...args);
  }

  /**
   * Execute a transactional function.
   * The transaction is guaranteed to execute exactly once, even if the workflow is retried with the same UUID.
   * If the transaction encounters a Postgres serialization error, retry it.
   * If it encounters any other error, throw it.
   */
  async transaction<T extends unknown[], R>(txn: Transaction<T, R>, clsinst: ConfiguredInstance | null, ...args: T): Promise<R> {
    return this.#dbosExec.callTransactionFunction(txn, clsinst, this, ...args);
  }

  /**
   * Execute a step function.
   * If it encounters any error, retry according to its configured retry policy until the maximum number of attempts is reached, then throw an DBOSError.
   * The step may execute many times, but once it is complete, it will not re-execute.
   */
  async external<T extends unknown[], R>(stepFn: StepFunction<T, R>, clsInst: ConfiguredInstance | null, ...args: T): Promise<R> {
    return this.#dbosExec.callStepFunction(stepFn, clsInst, this, ...args);
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
    const timeoutFunctionID: number = this.functionIDGetIncrement();

    await this.#dbosExec.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await this.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    this.resultBuffer.clear();

    return this.#dbosExec.systemDatabase.recv(this.workflowUUID, functionID, timeoutFunctionID, topic, timeoutSeconds);
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
            ? (...args: unknown[]) => this.external(op.registeredFunction as StepFunction<unknown[], unknown>, null, ...args)
            : op.procConfig
              ? (...args: unknown[]) => this.procedure(op.registeredFunction as StoredProcedure<unknown[], unknown>, ...args)
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
            ? (...args: unknown[]) => this.external(op.registeredFunction as StepFunction<unknown[], unknown>, targetInst, ...args)
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
    const timeoutFunctionID = this.functionIDGetIncrement();
    const params = {
      workflowUUID: this.workflowUUID,
      functionID,
      timeoutFunctionID
    };
    return this.#dbosExec.systemDatabase.getEvent(targetUUID, key, timeoutSeconds, params);
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
  /**
   * Return the workflow's inputs
   */
  getWorkflowInputs<T extends any[]>(): Promise<T>
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

  async getWorkflowInputs<T extends any[]>(): Promise<T> {
    return await this.systemDatabase.getWorkflowInputs<T>(this.workflowUUID) as T;
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

  async getWorkflowInputs<T extends any[]>(): Promise<T> {
    return await this.systemDatabase.getWorkflowInputs<T>(this.workflowUUID) as T;
  }
}
