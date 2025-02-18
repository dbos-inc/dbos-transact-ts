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

export interface DBOSEventReceiverRegistration {
  methodConfig: unknown;
  classConfig: unknown;
  methodReg: MethodRegistrationBase;
}

/*
 * Info provided to an event receiver at initialization,
 *  which contains the things that it needs to do its work
 *  (retrieve decorated endpoints, and run new transactions / workflows)
 */
export interface DBOSExecutorContext {
  /* Logging service */
  readonly logger: Logger;
  /* Tracing service */
  readonly tracer: Tracer;

  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;

  /*
   * Get the registrations for a receiver; this comes with:
   *  methodConfig: the method info the receiver stored
   *  classConfig: the class info the receiver stored
   *  methodReg: the method registration (w/ workflow, transaction, function, and other info)
   */
  getRegistrationsFor(eri: DBOSEventReceiver): DBOSEventReceiverRegistration[];

  transaction<T extends unknown[], R>(txn: TransactionFunction<T, R>, params: WorkflowParams, ...args: T): Promise<R>;
  workflow<T extends unknown[], R>(
    wf: WorkflowFunction<T, R>,
    params: WorkflowParams,
    ...args: T
  ): Promise<WorkflowHandle<R>>;
  external<T extends unknown[], R>(stepFn: StepFunction<T, R>, params: WorkflowParams, ...args: T): Promise<R>;
  procedure<T extends unknown[], R>(proc: StoredProcedure<T, R>, params: WorkflowParams, ...args: T): Promise<R>;

  send<T>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void>;
  getEvent<T>(workflowUUID: string, key: string, timeoutSeconds?: number): Promise<T | null>;
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;
  flushWorkflowBuffers(): Promise<void>;
  getWorkflowStatus(workflowID: string): Promise<WorkflowStatus | null>;
  getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput>;
  getWorkflowQueue(input: GetWorkflowQueueInput): Promise<GetWorkflowQueueOutput>;
  cancelWorkflow(workflowID: string): Promise<void>;

  // Event receiver state queries / updates
  /*
   * An event dispatcher may keep state in the system database
   *  The 'service' should be unique to the event receiver keeping state, to separate from others
   *   The 'workflowFnName' workflow function name should be the fully qualified / unique function name dispatched
   *   The 'key' field allows multiple records per service / workflow function
   *   The service+workflowFnName+key uniquely identifies the record, which is associated with:
   *     'value' - a value set by the event receiver service; this string may be a JSON to keep complex details
   *     A version, either as a sequence number (long integer), or as a time (high precision floating point).
   *       If versions are in use, any upsert is discarded if the version field is less than what is already stored.
   *       The upsert returns the current record, which is useful if it is more recent.
   */
  getEventDispatchState(
    service: string,
    workflowFnName: string,
    key: string,
  ): Promise<DBOSEventReceiverState | undefined>;
  upsertEventDispatchState(state: DBOSEventReceiverState): Promise<DBOSEventReceiverState>;

  queryUserDB(sql: string, params?: unknown[]): Promise<unknown[]>;

  userDBListen(channels: string[], callback: DBNotificationCallback): Promise<DBNotificationListener>;
}

/*
 * Interface for receiving events
 *  This is for things like kafka, SQS, etc., that poll for events and dispatch workflows
 * Needs to be:
 *  Registered with DBOS executor if any decorated endpoints need it
 *  Initialized / destroyed with the executor
 * It is the implememnter's job to keep going and dispatch workflows between those times
 */
export interface DBOSEventReceiver {
  executor?: DBOSExecutorContext;
  destroy(): Promise<void>;
  initialize(executor: DBOSExecutorContext): Promise<void>;
  logRegisteredEndpoints(): void;
}

export interface DBOSEventReceiverState {
  service: string;
  workflowFnName: string;
  key: string;
  value?: string;
  updateTime?: number;
  updateSeq?: bigint;
}

export interface DBOSEventReceiverQuery {
  service?: string;
  workflowFnName?: string;
  key?: string;
  startTime?: number;
  endTime?: number;
  startSeq?: bigint;
  endSeq?: bigint;
}
