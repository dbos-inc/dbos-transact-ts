/* eslint-disable @typescript-eslint/no-explicit-any */
import { DBOSExecutor, OperationType } from './dbos-executor';
import { SystemDatabase, WorkflowStatusInternal } from './system_database';
import { DBOSContext, DBOSContextImpl } from './context';
import { ConfiguredInstance } from './decorators';
import { DBOSJSON } from './utils';
import { DBOS, runInternalStep } from './dbos';
import { EnqueueOptions } from './system_database';

/** @deprecated */
export type Workflow<T extends unknown[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;
/** @deprecated */
export type WorkflowFunction<T extends unknown[], R> = Workflow<T, R>;

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

/**
 * @deprecated This class is no longer necessary
 * To update to Transact 2.0+
 *   Remove `WorkflowContext` from function parameter lists
 *   Use `DBOS.` to access DBOS context within affected functions
 *   Adjust callers to call the function directly, or to use `DBOS.startWorkflow`
 */
export interface WorkflowContext extends DBOSContext {
  foo?: () => void;
}

export class WorkflowContextImpl extends DBOSContextImpl implements WorkflowContext {
  constructor(
    dbosExec: DBOSExecutor,
    parentCtx: DBOSContextImpl | undefined,
    workflowUUID: string,
    readonly workflowConfig: WorkflowConfig,
    workflowName: string,
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
