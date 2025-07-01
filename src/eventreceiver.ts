import { Tracer } from './telemetry/traces';
import { GlobalLogger as Logger } from './telemetry/logs';
import type {
  GetQueuedWorkflowsInput,
  GetWorkflowsInput,
  StepInfo,
  WorkflowHandle,
  WorkflowParams,
  WorkflowStatus,
} from './workflow';
import type { Notification } from 'pg';

export type DBNotification = Notification;
export type DBNotificationCallback = (n: DBNotification) => void;
export interface DBNotificationListener {
  close(): Promise<void>;
}

/*
 * Info provided to an event receiver at initialization,
 *  which contains the things that it needs to do its work
 *  (retrieve decorated endpoints, and run new transactions / workflows)
 */
export interface DBOSExecutorContext {
  /** Logging service; @deprecated: Use `DBOS.logger` instead. */
  readonly logger: Logger;
  /** Tracing service; @deprecated: Use `DBOS.tracer` instead.  */
  readonly tracer: Tracer;

  /** @deprecated */
  getConfig<T>(key: string): T | undefined;
  /** @deprecated */
  getConfig<T>(key: string, defaultValue: T): T;

  /**
   * Invoke a transaction function.
   *  Note that functions can be called directly instead of using this interface.
   */
  transaction<T extends unknown[], R>(txn: (...args: T) => Promise<R>, params: WorkflowParams, ...args: T): Promise<R>;

  /**
   * Invoke a workflow function.
   *  Note that functions can be enqueued directly with `DBOS.startWorkflow` instead of using this interface.
   */
  workflow<T extends unknown[], R>(
    wf: (...args: T) => Promise<R>,
    params: WorkflowParams,
    ...args: T
  ): Promise<WorkflowHandle<R>>;

  /**
   * @deprecated
   * Invoke a step function.
   *  Note that functions can be called directly instead of using this interface.
   */
  external<T extends unknown[], R>(func: (...args: T) => Promise<R>, params: WorkflowParams, ...args: T): Promise<R>;

  /**
   * @deprecated
   * Invoke a stored procedure function.
   *  Note that functions can be called directly instead of using this interface.
   */
  procedure<T extends unknown[], R>(proc: (...args: T) => Promise<R>, params: WorkflowParams, ...args: T): Promise<R>;

  /**
   * Send a messsage to workflow with a given ID.
   * @deprecated `DBOS.send` can be used instead.
   * @param destinationID - ID of workflow to receive the message
   * @param message - Message
   * @param topic - Topic for message
   * @param idempotencyKey - For sending exactly once
   * @template T - Type of object to send
   */
  send<T>(destinationID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void>;
  /**
   * Get an event set by a workflow.
   * @deprecated Note that `DBOS.getEvent` can be used instead.
   */
  getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null>;
  /**
   * Retrieve a workflow handle given the workflow ID.
   * @deprecated Note that `DBOS.retrieveWorkflow` can be used instead
   */
  retrieveWorkflow<R>(workflowID: string): WorkflowHandle<R>;
  /** @deprecated Use functions on `DBOS` */
  getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatus | null>;
  /** @deprecated Use functions on `DBOS` */
  listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]>;
  /** @deprecated Use functions on `DBOS` */
  listQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<WorkflowStatus[]>;
  /** @deprecated Use functions on `DBOS` */
  listWorkflowSteps(workflowID: string): Promise<StepInfo[] | undefined>;
  /** @deprecated Use functions on `DBOS` */
  cancelWorkflow(workflowID: string): Promise<void>;
  /** @deprecated Use functions on `DBOS` */
  resumeWorkflow(workflowID: string): Promise<void>;
  /** @deprecated Use functions on `DBOS` */
  forkWorkflow(
    workflowID: string,
    startStep: number,
    options?: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number },
  ): Promise<string>;

  // Event receiver state queries / updates
  /** @deprecated see DBOS.getEventDispatchState */
  getEventDispatchState(
    service: string,
    workflowFnName: string,
    key: string,
  ): Promise<DBOSEventReceiverState | undefined>;

  /** @deprecated see DBOS.upsertEventDispatchState */
  upsertEventDispatchState(state: DBOSEventReceiverState): Promise<DBOSEventReceiverState>;

  /** @deprecated Use `DBOS.queryUserDB` */
  queryUserDB(sql: string, params?: unknown[]): Promise<unknown[]>;

  /** @deprecated Listen for notifications from the user DB */
  userDBListen(channels: string[], callback: DBNotificationCallback): Promise<DBNotificationListener>;
}

/**
 * State item to be kept in the DBOS system database on behalf of clients
 */
export interface DBOSEventReceiverState {
  /** Name of event receiver service */
  service: string;
  /** Fully qualified function name for which state is kept */
  workflowFnName: string;
  /** subkey within the service+workflowFnName */
  key: string;
  /** Value kept for the service+workflowFnName+key combination */
  value?: string;
  /** Updated time (used to version the value) */
  updateTime?: number;
  /** Updated sequence number (used to version the value) */
  updateSeq?: bigint;
}
