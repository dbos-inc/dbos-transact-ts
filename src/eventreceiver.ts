import { Tracer } from './telemetry/traces';
import { GlobalLogger as Logger } from './telemetry/logs';
import { WorkflowFunction, WorkflowHandle, WorkflowParams } from './workflow';
import { TransactionFunction } from './transaction';
import { MethodRegistrationBase } from './decorators';
import { CommunicatorFunction } from './communicator';

/*
 * Info provided to an event receiver at initialization,
 *  which contains the things that it needs to do its work
 *  (retrieve decorated endpoints, and run new transactions / workflows)
 */
export interface DBOSExecutorContext
{
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
  getRegistrationsFor(eri: DBOSEventReceiver) : {methodConfig: unknown, classConfig: unknown, methodReg: MethodRegistrationBase}[];

  transaction<T extends unknown[], R>(txn: TransactionFunction<T, R>, params: WorkflowParams, ...args: T): Promise<R>;
  workflow<T extends unknown[], R>(wf: WorkflowFunction<T, R>, params: WorkflowParams, ...args: T): Promise<WorkflowHandle<R>>;
  external<T extends unknown[], R>(commFn: CommunicatorFunction<T, R>, params: WorkflowParams, ...args: T): Promise<R>;

  send<T>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void>;
  getEvent<T>(workflowUUID: string, key: string, timeoutSeconds: number): Promise<T | null>;
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;
}

/*
 * Interface for receiving events
 *  This is for things like kafka, SQS, etc., that poll for events and dispatch workflows
 * Needs to be:
 *  Registered with DBOS executor if any decorated endpoints need it
 *  Initialized / destroyed with the executor
 * It is the implememnter's job to keep going and dispatch workflows between those times
 */
export interface DBOSEventReceiver
{
    executor ?: DBOSExecutorContext;
    destroy() : Promise<void>;
    initialize(executor: DBOSExecutorContext) : Promise<void>;
    logRegisteredEndpoints() : void;
}