/* eslint-disable @typescript-eslint/no-explicit-any */
import { DBOSExecutor, OperationType } from './dbos-executor';
import { StepFunction } from './step';
import { SystemDatabase, WorkflowStatusInternal } from './system_database';
import { DBOSContext, DBOSContextImpl, functionIDGetIncrement } from './context';
import { ConfiguredInstance, getRegisteredOperations } from './decorators';
import { WorkflowQueue } from './wfqueue';
import { DBOSJSON } from './utils';
import { DBOS, runInternalStep } from './dbos';
import { EnqueueOptions } from './system_database';

/** @deprecated */
export type Workflow<T extends unknown[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;
/** @deprecated */
export type WorkflowFunction<T extends unknown[], R> = Workflow<T, R>;
export type ContextFreeFunction<T extends unknown[], R> = (...args: T) => Promise<R>;

// Utility type that removes the initial parameter of a function
export type TailParameters<T extends (arg: any, args: any[]) => any> = T extends (arg: any, ...args: infer P) => any
  ? P
  : never;

export interface WorkflowParams {
  workflowUUID?: string;
  parentCtx?: DBOSContextImpl;
  configuredInstance?: ConfiguredInstance | null;
  queueName?: string;
  executeWorkflow?: boolean; // If queueName is set, this will not be run unless executeWorkflow is true.
  timeoutMS?: number | null;
  deadlineEpochMS?: number;
  enqueueOptions?: EnqueueOptions; // Options for the workflow queue
}

/**
 * Configuration for `DBOS.workflow` functions
 */
export interface WorkflowConfig {
  /** Maximum number of recovery attempts to make on workflow function, before sending to dead-letter queue */
  maxRecoveryAttempts?: number;
}

export interface WorkflowStatus {
  readonly workflowID: string;
  readonly status: string; // The status of the workflow.  One of PENDING, SUCCESS, ERROR, RETRIES_EXCEEDED, ENQUEUED, or CANCELLED.
  readonly workflowName: string; // The name of the workflow function.
  readonly workflowClassName: string; // The class name holding the workflow function.
  readonly workflowConfigName?: string; // The name of the configuration, if the class needs configuration
  readonly queueName?: string; // The name of the queue, if workflow was queued

  readonly authenticatedUser?: string; // The user who ran the workflow. Empty string if not set.
  readonly assumedRole?: string; // The role used to run this workflow.  Empty string if authorization is not required.
  readonly authenticatedRoles?: string[]; // All roles the authenticated user has, if any.

  readonly output?: unknown;
  readonly error?: unknown; // The error thrown by the workflow, if any.
  readonly input?: unknown[]; // The input to the workflow, if any.

  readonly request?: object; // The parent request for this workflow, if any.
  readonly executorId?: string; // The ID of the workflow executor
  readonly applicationVersion?: string;
  readonly applicationID: string;
  readonly recoveryAttempts?: number;

  readonly createdAt: number;
  readonly updatedAt?: number;

  readonly timeoutMS?: number | null;
  readonly deadlineEpochMS?: number;
}

export interface GetWorkflowsInput {
  workflowIDs?: string[]; // The workflow IDs to retrieve
  workflowName?: string; // The name of the workflow function
  authenticatedUser?: string; // The user who ran the workflow.
  startTime?: string; // Timestamp in ISO 8601 format
  endTime?: string; // Timestamp in ISO 8601 format
  status?: 'PENDING' | 'SUCCESS' | 'ERROR' | 'RETRIES_EXCEEDED' | 'CANCELLED' | 'ENQUEUED'; // The status of the workflow.
  applicationVersion?: string; // The application version that ran this workflow.
  limit?: number; // Return up to this many workflows IDs. IDs are ordered by workflow creation time.
  offset?: number; // Skip this many workflows IDs. IDs are ordered by workflow creation time.
  sortDesc?: boolean; // Sort the workflows in descending order by creation time (default ascending order).
  workflow_id_prefix?: string;
}

export interface GetQueuedWorkflowsInput {
  workflowName?: string; // The name of the workflow function
  startTime?: string; // Timestamp in ISO 8601 format
  endTime?: string; // Timestamp in ISO 8601 format
  status?: 'PENDING' | 'SUCCESS' | 'ERROR' | 'RETRIES_EXCEEDED' | 'CANCELLED' | 'ENQUEUED'; // The status of the workflow.
  limit?: number; // Return up to this many workflows IDs. IDs are ordered by workflow creation time.
  queueName?: string; // The queue
  offset?: number; // Skip this many workflows IDs. IDs are ordered by workflow creation time.
  sortDesc?: boolean; // Sort the workflows in descending order by creation time (default ascending order).
}

export interface GetWorkflowsOutput {
  workflowUUIDs: string[];
}

export interface GetPendingWorkflowsOutput {
  workflowUUID: string;
  queueName?: string;
}

export interface StepInfo {
  readonly functionID: number;
  readonly name: string;
  readonly output: unknown;
  readonly error: Error | null;
  readonly childWorkflowID: string | null;
}

export interface PgTransactionId {
  txid: string;
}

/** Enumeration of values for workflow status */
export const StatusString = {
  /** Workflow has may be running */
  PENDING: 'PENDING',
  /** Workflow complete with return value */
  SUCCESS: 'SUCCESS',
  /** Workflow complete with error thrown */
  ERROR: 'ERROR',
  /** Workflow has been retried the maximum number of times, without completing (SUCCESS/ERROR) */
  RETRIES_EXCEEDED: 'RETRIES_EXCEEDED',
  /** Workflow is being, or has been, cancelled */
  CANCELLED: 'CANCELLED',
  /** Workflow is on a `WorkflowQueue` and has not yet started */
  ENQUEUED: 'ENQUEUED',
} as const;

type WFFunc = (ctxt: WorkflowContext, ...args: any[]) => Promise<unknown>;
export type WfInvokeWfs<T> = {
  [P in keyof T]: T[P] extends WFFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
};
export type WfInvokeWfsAsync<T> = {
  [P in keyof T]: T[P] extends WFFunc
    ? (...args: TailParameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>>
    : never;
};

export type WfInvokeWfsInst<T> = T extends ConfiguredInstance
  ? {
      [P in keyof T]: T[P] extends WFFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
    }
  : never;

export type WfInvokeWfsInstAsync<T> = T extends ConfiguredInstance
  ? {
      [P in keyof T]: T[P] extends WFFunc
        ? (...args: TailParameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>>
        : never;
    }
  : never;

/**
 * @deprecated This class is no longer necessary
 * To update to Transact 2.0+
 *   Remove `WorkflowContext` from function parameter lists
 *   Use `DBOS.` to access DBOS context within affected functions
 *   Adjust callers to call the function directly, or to use `DBOS.startWorkflow`
 */
export interface WorkflowContext extends DBOSContext {
  startWorkflow<T extends ConfiguredInstance>(
    targetClass: T,
    workflowUUID?: string,
    queue?: WorkflowQueue,
  ): WfInvokeWfsInstAsync<T>;
  startWorkflow<T extends object>(targetClass: T, workflowUUID?: string, queue?: WorkflowQueue): WfInvokeWfsAsync<T>;

  send<T>(destinationID: string, message: T, topic?: string): Promise<void>;
  recv<T>(topic?: string, timeoutSeconds?: number): Promise<T | null>;
  setEvent<T>(key: string, value: T): Promise<void>;
  getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null>;

  retrieveWorkflow<R>(workflowID: string): WorkflowHandle<R>;

  sleepms(durationMS: number): Promise<void>;
  sleep(durationSec: number): Promise<void>;
}

export class WorkflowContextImpl extends DBOSContextImpl implements WorkflowContext {
  readonly #dbosExec;
  readonly isTempWorkflow: boolean;
  readonly maxRecoveryAttempts;

  constructor(
    dbosExec: DBOSExecutor,
    parentCtx: DBOSContextImpl | undefined,
    workflowUUID: string,
    readonly workflowConfig: WorkflowConfig,
    workflowName: string,
    readonly presetUUID: boolean,
    readonly timeoutMS: number | undefined | null,
    readonly deadlineEpochMS: number | undefined,
    readonly tempWfOperationType: string = '', // "transaction", "procedure", "external", or "send"
    readonly tempWfOperationName: string = '', // Name for the temporary workflow operation
  ) {
    const span = dbosExec.tracer.startSpan(
      workflowName,
      {
        status: StatusString.PENDING,
        operationUUID: workflowUUID,
        operationType: OperationType.WORKFLOW,
        authenticatedUser: parentCtx?.authenticatedUser ?? '',
        authenticatedRoles: parentCtx?.authenticatedRoles ?? [],
        assumedRole: parentCtx?.assumedRole ?? '',
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

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to use WorkflowContext.Transaction(OpClass.someMethod, param);
   */
  proxyInvokeWF<T extends object>(
    object: T,
    workflowUUID: string | undefined,
    asyncWf: boolean,
    configuredInstance: ConfiguredInstance | null,
    queue?: WorkflowQueue,
  ): WfInvokeWfsAsync<T> {
    const ops = getRegisteredOperations(object);
    const proxy: Record<string, unknown> = {};

    const funcId = functionIDGetIncrement();
    const childUUID = workflowUUID || this.workflowUUID + '-' + funcId;

    const params = { workflowUUID: childUUID, parentCtx: this, configuredInstance, queueName: queue?.name };

    for (const op of ops) {
      if (asyncWf) {
        proxy[op.name] = op.workflowConfig
          ? (...args: unknown[]) =>
              this.#dbosExec.internalWorkflow(
                op.registeredFunction as Workflow<unknown[], unknown>,
                params,
                this.workflowUUID,
                funcId,
                ...args,
              )
          : undefined;
      } else {
        proxy[op.name] = op.workflowConfig
          ? (...args: unknown[]) =>
              this.#dbosExec
                .internalWorkflow(
                  op.registeredFunction as Workflow<unknown[], unknown>,
                  params,
                  this.workflowUUID,
                  funcId,
                  ...args,
                )
                .then((handle) => handle.getResult())
          : undefined;
      }
    }
    return proxy as WfInvokeWfsAsync<T>;
  }

  startWorkflow<T extends object>(target: T, workflowUUID?: string, queue?: WorkflowQueue): WfInvokeWfsAsync<T> {
    if (typeof target === 'function') {
      return this.proxyInvokeWF(target, workflowUUID, true, null, queue) as unknown as WfInvokeWfsAsync<T>;
    } else {
      return this.proxyInvokeWF(
        target,
        workflowUUID,
        true,
        target as ConfiguredInstance,
        queue,
      ) as unknown as WfInvokeWfsAsync<T>;
    }
  }

  // TODO: ConfiguredInstance support
  async procedure<T extends unknown[], R>(proc: (...args: T) => Promise<R>, ...args: T): Promise<R> {
    return this.#dbosExec.callProcedureFunction(proc, this, ...args);
  }

  /**
   * Execute a transactional function.
   * The transaction is guaranteed to execute exactly once, even if the workflow is retried with the same UUID.
   * If the transaction encounters a Postgres serialization error, retry it.
   * If it encounters any other error, throw it.
   */
  async transaction<T extends unknown[], R>(
    txn: (...args: T) => Promise<R>,
    clsinst: ConfiguredInstance | null,
    ...args: T
  ): Promise<R> {
    return this.#dbosExec.callTransactionFunction(txn, clsinst, this, ...args);
  }

  /**
   * Execute a step function.
   * If it encounters any error, retry according to its configured retry policy until the maximum number of attempts is reached, then throw an DBOSError.
   * The step may execute many times, but once it is complete, it will not re-execute.
   */
  async external<T extends unknown[], R>(
    stepFn: StepFunction<T, R>,
    clsInst: ConfiguredInstance | null,
    ...args: T
  ): Promise<R> {
    return this.#dbosExec.callStepFunction(stepFn, undefined, undefined, clsInst, this, ...args);
  }

  /**
   * Send a message to a workflow identified by a UUID.
   * The message can optionally be tagged with a topic.
   */
  async send<T>(destinationUUID: string, message: T, topic?: string): Promise<void> {
    const functionID: number = functionIDGetIncrement();
    await this.#dbosExec.systemDatabase.send(
      this.workflowUUID,
      functionID,
      destinationUUID,
      DBOSJSON.stringify(message),
      topic,
    );
  }

  /**
   * Consume and return the oldest unconsumed message sent to your UUID.
   * If a topic is specified, retrieve the oldest message tagged with that topic.
   * Otherwise, retrieve the oldest message with no topic.
   */
  async recv<T>(
    topic?: string,
    timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec,
  ): Promise<T | null> {
    const functionID: number = functionIDGetIncrement();
    const timeoutFunctionID: number = functionIDGetIncrement();
    return DBOSJSON.parse(
      await this.#dbosExec.systemDatabase.recv(this.workflowUUID, functionID, timeoutFunctionID, topic, timeoutSeconds),
    ) as T;
  }

  /**
   * Emit a workflow event, represented as a key-value pair.
   * Events are immutable once set.
   */
  async setEvent<T>(key: string, value: T) {
    const functionID = functionIDGetIncrement();
    await this.#dbosExec.systemDatabase.setEvent(this.workflowUUID, functionID, key, DBOSJSON.stringify(value));
  }

  /**
   * Wait for a workflow to emit an event, then return its value.
   */
  async getEvent<T>(
    targetUUID: string,
    key: string,
    timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec,
  ): Promise<T | null> {
    const functionID: number = functionIDGetIncrement();
    const timeoutFunctionID = functionIDGetIncrement();
    const params = {
      workflowID: this.workflowUUID,
      functionID,
      timeoutFunctionID,
    };
    return DBOSJSON.parse(await this.#dbosExec.systemDatabase.getEvent(targetUUID, key, timeoutSeconds, params)) as T;
  }

  /**
   * Retrieve a handle for a workflow UUID.
   */
  retrieveWorkflow<R>(targetID: string): WorkflowHandle<R> {
    const functionID: number = functionIDGetIncrement();
    return new RetrievedHandle(this.#dbosExec.systemDatabase, targetID, this.workflowUUID, functionID);
  }

  /**
   * Sleep for the duration.
   */
  async sleepms(durationMS: number): Promise<void> {
    if (durationMS <= 0) {
      return;
    }
    const functionID = functionIDGetIncrement();
    await this.#dbosExec.systemDatabase.durableSleepms(this.workflowUUID, functionID, durationMS);
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
   * @deprecated use `.workflowID` instead of `.getWorkflowUUID()`
   * Return the workflow's ID, which may be a UUID (but not necessarily).
   */
  getWorkflowUUID(): string;
  /**
   * Return the workflow's ID
   */
  get workflowID(): string;
  /**
   * Return the workflow's inputs
   */
  getWorkflowInputs<T extends any[]>(): Promise<T>;
}

/**
 * The handle returned when invoking a workflow with DBOSExecutor.workflow
 */
export class InvokedHandle<R> implements WorkflowHandle<R> {
  constructor(
    readonly systemDatabase: SystemDatabase,
    readonly workflowPromise: Promise<R>,
    readonly workflowUUID: string,
    readonly workflowName: string,
    readonly callerUUID?: string, // This is the call that started the WF
    readonly callerFunctionID?: number, // This is the call that started the WF
  ) {}

  getWorkflowUUID(): string {
    return this.workflowUUID;
  }

  get workflowID(): string {
    return this.workflowUUID;
  }

  async getStatus(): Promise<WorkflowStatus | null> {
    return await DBOS.getWorkflowStatus(this.workflowUUID);
  }

  async getResult(): Promise<R> {
    return await runInternalStep(
      async () => {
        return await this.workflowPromise;
      },
      'DBOS.getResult',
      this.workflowUUID,
    );
  }

  async getWorkflowInputs<T extends any[]>(): Promise<T> {
    const status = (await this.systemDatabase.getWorkflowStatus(this.workflowUUID)) as WorkflowStatusInternal;
    return DBOSJSON.parse(status.input) as T;
  }
}

/**
 * The handle returned when retrieving a workflow with DBOSExecutor.retrieve
 */
export class RetrievedHandle<R> implements WorkflowHandle<R> {
  constructor(
    readonly systemDatabase: SystemDatabase,
    readonly workflowUUID: string,
    readonly callerUUID?: string,
    readonly callerFunctionID?: number,
  ) {}

  getWorkflowUUID(): string {
    return this.workflowUUID;
  }

  get workflowID(): string {
    return this.workflowUUID;
  }

  async getStatus(): Promise<WorkflowStatus | null> {
    return await DBOS.getWorkflowStatus(this.workflowUUID);
  }

  async getResult(): Promise<R> {
    return (await DBOS.getResult<R>(this.workflowUUID)) as Promise<R>;
  }

  async getWorkflowInputs<T extends any[]>(): Promise<T> {
    const status = (await this.systemDatabase.getWorkflowStatus(this.workflowUUID)) as WorkflowStatusInternal;
    return DBOSJSON.parse(status.input) as T;
  }
}
