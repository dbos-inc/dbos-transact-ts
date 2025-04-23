import { Tracer } from './telemetry/traces';
import { GlobalLogger as Logger } from './telemetry/logs';
import {
  GetWorkflowQueueInput,
  GetWorkflowQueueOutput,
  GetWorkflowsInput,
  GetWorkflowsOutput,
  WorkflowFunction,
  WorkflowHandle,
  WorkflowParams,
  WorkflowStatus,
} from './workflow';
import { TransactionFunction } from './transaction';
import { MethodRegistrationBase } from './decorators';
import { StepFunction } from './step';
import { Notification } from 'pg';
import { StoredProcedure } from './procedure';

export type DBNotification = Notification;
export type DBNotificationCallback = (n: DBNotification) => void;
export interface DBNotificationListener {
  close(): Promise<void>;
}

/**
 * Interface for registration information provided to `DBOSEventReceiver`
 *  instances about each method and containing class.  This contains any
 *  information stored by the decorators that registered the method with
 *  its event receiver.
 */
export interface DBOSEventReceiverRegistration {
  /** Method-level configuration information, as stored by the event receiver's decorators (or other registration mechanism) */
  methodConfig: unknown;
  /** Class-level configuration information, as stored by the event receiver's decorators (or other registration mechanism) */
  classConfig: unknown;
  /** Method to dispatch, and associated DBOS registration information */
  methodReg: MethodRegistrationBase;
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
   * Get the registrations for a receiver
   * @param eri - all registrations for this `DBOSEventReceiver` will be returned
   * @returns array of all methods registered for the receiver, including:
   *  methodConfig: the method info the receiver stored
   *  classConfig: the class info the receiver stored
   *  methodReg: the method registration (w/ workflow, transaction, function, and other info)
   */
  getRegistrationsFor(eri: DBOSEventReceiver): DBOSEventReceiverRegistration[];

  /**
   * Invoke a transaction function.
   *  Note that functions can be called directly instead of using this interface.
   */
  transaction<T extends unknown[], R>(txn: TransactionFunction<T, R>, params: WorkflowParams, ...args: T): Promise<R>;

  /**
   * Invoke a workflow function.
   *  Note that functions can be enqueued directly with `DBOS.startWorkflow` instead of using this interface.
   */
  workflow<T extends unknown[], R>(
    wf: WorkflowFunction<T, R>,
    params: WorkflowParams,
    ...args: T
  ): Promise<WorkflowHandle<R>>;

  /**
   * @deprecated
   * Invoke a step function.
   *  Note that functions can be called directly instead of using this interface.
   */
  external<T extends unknown[], R>(stepFn: StepFunction<T, R>, params: WorkflowParams, ...args: T): Promise<R>;

  /**
   * @deprecated
   * Invoke a stored procedure function.
   *  Note that functions can be called directly instead of using this interface.
   */
  procedure<T extends unknown[], R>(proc: StoredProcedure<T, R>, params: WorkflowParams, ...args: T): Promise<R>;

  /**
   * Send a messsage to workflow with a given ID.  Note that `DBOS.send` can be used instead.
   * @param destinationID - ID of workflow to receive the message
   * @param message - Message
   * @param topic - Topic for message
   * @param idempotencyKey - For sending exactly once
   * @template T - Type of object to send
   */
  send<T>(destinationID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void>;
  /** Get an event set by a workflow.  Note that `DBOS.getEvent` can be used instead. */
  getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null>;
  /** Retrieve a workflow handle given the workflow ID.  Note that `DBOS.retrieveWorkflow` can be used instead */
  retrieveWorkflow<R>(workflowID: string): WorkflowHandle<R>;
  /** @deprecated Use functions on `DBOS` */
  getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatus | null>;
  /** @deprecated Use functions on `DBOS` */
  getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput>;
  /** @deprecated Use functions on `DBOS` */
  getWorkflowQueue(input: GetWorkflowQueueInput): Promise<GetWorkflowQueueOutput>;
  /** @deprecated Use functions on `DBOS` */
  cancelWorkflow(workflowID: string): Promise<void>;
  /** @deprecated Use functions on `DBOS` */
  resumeWorkflow(workflowID: string): Promise<void>;

  forkWorkflow(workflowID: string): Promise<string>;

  // Event receiver state queries / updates
  /** @see DBOS.getEventDispatchState */
  getEventDispatchState(
    service: string,
    workflowFnName: string,
    key: string,
  ): Promise<DBOSEventReceiverState | undefined>;

  /** @see DBOS.upsertEventDispatchState */
  upsertEventDispatchState(state: DBOSEventReceiverState): Promise<DBOSEventReceiverState>;

  /** @deprecated Use `DBOS.queryUserDB` */
  queryUserDB(sql: string, params?: unknown[]): Promise<unknown[]>;

  /** @deprecated Listen for notifications from the user DB */
  userDBListen(channels: string[], callback: DBNotificationCallback): Promise<DBNotificationListener>;
}

/**
 * Interface for DBOS pluggable event receivers.
 *  This is for things like kafka, SQS, HTTP, schedulers, etc., that listen or poll
 *    for events and dispatch workflows in response.
 * A `DBOSEventReceiver` will be:
 *  Registered with DBOS executor when any endpoint workflow function needs it
 *  Initialized with the executor during launch
 *  Destroyed upon any clean `shutdown()`
 * It is the implementer's job to keep going and dispatch workflows between
 *  `initialize` and `destroy`
 */
export interface DBOSEventReceiver {
  /** Executor, for providing state and dispatching DBOS workflows or other methods */
  executor?: DBOSExecutorContext;
  /** Called upon shutdown (usually in tests) to stop event receivers and free resources */
  destroy(): Promise<void>;
  /** Called during DBOS launch to indicate that event receiving should start */
  initialize(executor: DBOSExecutorContext): Promise<void>;
  /** Called at launch; Implementers should emit a diagnostic list of all registrations */
  logRegisteredEndpoints(): void;
}

/**
 * State item to be kept in the DBOS system database on behalf of `DBOSEventReceiver`s
 * @see DBOSEventReceiver.upsertEventDispatchState
 * @see DBOSEventReceiver.getEventDispatchState
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
