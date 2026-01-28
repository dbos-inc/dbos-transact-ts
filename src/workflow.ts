/* eslint-disable @typescript-eslint/no-explicit-any */
import { SystemDatabase, WorkflowStatusInternal } from './system_database';
import { ConfiguredInstance } from './decorators';
import { registerSerializationRecipe } from './serialization';
import { DBOS, runInternalStep } from './dbos';
import { EnqueueOptions } from './system_database';
import { DBOSExecutor } from './dbos-executor';

export interface WorkflowParams {
  workflowUUID?: string;
  configuredInstance?: ConfiguredInstance | null;
  queueName?: string;
  executeWorkflow?: boolean; // If queueName is set, this will not be run unless executeWorkflow is true.
  timeoutMS?: number | null;
  deadlineEpochMS?: number;
  enqueueOptions?: EnqueueOptions; // Options for the workflow queue
}

export const DEFAULT_MAX_RECOVERY_ATTEMPTS = 100;

export type WorkflowSerializationFormat = undefined | 'native' | 'portable';

/**
 * Configuration for `DBOS.workflow` functions
 */
export interface WorkflowConfig {
  /** Maximum number of recovery attempts to make on workflow function, before sending to dead-letter queue */
  maxRecoveryAttempts?: number;
  /** Name to use */
  name?: string;
  /** Default serialization to use */
  serialization?: WorkflowSerializationFormat;
}

export interface WorkflowStatus {
  // The workflow ID
  readonly workflowID: string;
  // The status of the workflow.  One of PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED.
  readonly status: string;
  // The name of the workflow function.
  readonly workflowName: string;
  // The name of the workflow's class, if any.
  readonly workflowClassName: string;
  // The name with which the workflow's class instance was configured, if any.
  readonly workflowConfigName?: string;
  // If the workflow was enqueued, the name of the queue.
  readonly queueName?: string;

  // The user who ran the workflow, if set.
  readonly authenticatedUser?: string;
  // The role used to run the workflow, if set.
  readonly assumedRole?: string;
  // All roles the authenticated user has, if set.
  readonly authenticatedRoles?: string[];

  // The deserialized workflow inputs.
  readonly input?: unknown[];
  // The workflow's deserialized output, if any.
  readonly output?: unknown;
  // The error thrown by the workflow, if any.
  readonly error?: unknown;

  // The ID of the executor (process) that most recently executed this workflow.
  readonly executorId?: string;
  // The application version on which this workflow started.
  readonly applicationVersion?: string;

  // Workflow start time, as a UNIX epoch timestamp in milliseconds
  readonly createdAt: number;
  // Last time the workflow status was updated, as a UNIX epoch timestamp in milliseconds. For a completed workflow, this is the workflow completion timestamp.
  readonly updatedAt?: number;

  // The timeout specified for this workflow, if any. Timeouts are start-to-close.
  readonly timeoutMS?: number;
  // The deadline at which this workflow times out, if any. Not set until the workflow begins execution.
  readonly deadlineEpochMS?: number;
  // Unique queue deduplication ID, if any. Deduplication IDs are unset when the workflow completes.
  readonly deduplicationID?: string;
  // Priority of the workflow on a queue, starting from 1 ~ 2,147,483,647. Default 0 (highest priority).
  readonly priority: number;
  // If this workflow is enqueued on a partitioned queue, its partition key
  readonly queuePartitionKey?: string;

  // If this workflow was forked from another, that workflow's ID.
  readonly forkedFrom?: string;

  // INTERNAL
  // Deprecated field
  readonly applicationID: string;
  // Deprecated field
  readonly request?: object;
  // The number of times this workflow has been started.
  readonly recoveryAttempts?: number;
}

export interface GetWorkflowsInput {
  workflowIDs?: string[]; // Retrieve workflows with these IDs.
  workflowName?: string; // Retrieve workflows with this name.
  status?: 'PENDING' | 'SUCCESS' | 'ERROR' | 'MAX_RECOVERY_ATTEMPTS_EXCEEDED' | 'CANCELLED' | 'ENQUEUED'; // Retrieve workflows with this status (Must be `ENQUEUED`, `PENDING`, `SUCCESS`, `ERROR`, `CANCELLED`, or `RETRIES_EXCEEDED`)
  startTime?: string; // Retrieve workflows started after this (RFC 3339-compliant) timestamp.
  endTime?: string; // Retrieve workflows started before this (RFC 3339-compliant) timestamp.
  authenticatedUser?: string; // Retrieve workflows run by this authenticated user.
  applicationVersion?: string; // Retrieve workflows started on this application version.
  executorId?: string; // Retrieve workflows run by this executor ID.
  workflow_id_prefix?: string; // Retrieve workflows whose ID have this prefix
  queueName?: string; // If this workflow is enqueued, on which queue
  queuesOnly?: boolean; // Return only workflows that are actively enqueued
  forkedFrom?: string; // Get workflows forked from this workflow ID.
  limit?: number; // Return up to this many workflows IDs. IDs are ordered by workflow creation time.
  offset?: number; // Skip this many workflows IDs. IDs are ordered by workflow creation time.
  sortDesc?: boolean; // Sort the workflows in descending order by creation time (default ascending order).
  loadInput?: boolean; // Load the input of the workflow (default true)
  loadOutput?: boolean; // Load the output of the workflow (default true)
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
  readonly startedAtEpochMs?: number;
  readonly completedAtEpochMs?: number;
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
  /** Workflow has exceeded its maximum number of execution or recovery attempts */
  MAX_RECOVERY_ATTEMPTS_EXCEEDED: 'MAX_RECOVERY_ATTEMPTS_EXCEEDED',
  /** Workflow is being, or has been, cancelled */
  CANCELLED: 'CANCELLED',
  /** Workflow is on a `WorkflowQueue` and has not yet started */
  ENQUEUED: 'ENQUEUED',
} as const;

export function isWorkflowActive(status: string) {
  return status === StatusString.PENDING || status === StatusString.ENQUEUED;
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
   * Return the workflow's ID
   */
  get workflowID(): string;
  /**
   * Return the workflow's inputs
   */
  getWorkflowInputs<T extends any[]>(): Promise<T>;
}

export interface InternalWFHandle<R> extends WorkflowHandle<R> {
  getResult(funcIdForGet?: number): Promise<R>;
}

/**
 * The handle returned when invoking a workflow with DBOSExecutor.workflow
 */
export class InvokedHandle<R> implements InternalWFHandle<R> {
  constructor(
    readonly systemDatabase: SystemDatabase,
    readonly workflowPromise: Promise<R>,
    readonly workflowUUID: string,
    readonly workflowName: string,
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

  async getResult(funcIdForGet?: number): Promise<R> {
    return await runInternalStep(
      async () => {
        return await this.workflowPromise;
      },
      'DBOS.getResult',
      this.workflowUUID,
      funcIdForGet,
    );
  }

  async getWorkflowInputs<T extends any[]>(): Promise<T> {
    const status = (await this.systemDatabase.getWorkflowStatus(this.workflowUUID)) as WorkflowStatusInternal;
    return this.systemDatabase.getSerializer().parse(status.input) as T;
  }
}

/**
 * The handle returned when retrieving a workflow with DBOSExecutor.retrieve
 */
export class RetrievedHandle<R> implements InternalWFHandle<R> {
  constructor(
    readonly systemDatabase: SystemDatabase,
    readonly workflowUUID: string,
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

  async getResult(funcIdForGet?: number): Promise<R> {
    return (await DBOS.getResultInternal<R>(this.workflowUUID, undefined, undefined, funcIdForGet)) as Promise<R>;
  }

  async getWorkflowInputs<T extends any[]>(): Promise<T> {
    const status = (await this.systemDatabase.getWorkflowStatus(this.workflowUUID)) as WorkflowStatusInternal;
    return this.systemDatabase.getSerializer().parse(status.input) as T;
  }
}

registerSerializationRecipe<WorkflowHandle<unknown>, { wfid: string }>({
  name: 'DBOS.WorkflowHandle',
  isApplicable: (v: unknown): v is WorkflowHandle<unknown> => {
    return v instanceof RetrievedHandle || v instanceof InvokedHandle;
  },
  serialize: (v: WorkflowHandle<unknown>) => {
    return { wfid: v.workflowID };
  },
  deserialize: (s: { wfid: string }) => new RetrievedHandle(DBOSExecutor.globalInstance!.systemDatabase, s.wfid),
});
