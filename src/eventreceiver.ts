import { Tracer } from './telemetry/traces';
import { GlobalLogger as Logger } from './telemetry/logs';
import { WorkflowFunction, WorkflowHandle, WorkflowParams } from './workflow';
import { Transaction } from './transaction';
import { MethodRegistrationBase } from './decorators';

/*
 * Info provided to an event receiver at initialization,
 *  which contains the things that it needs to do its work
 *  (retrieve decorated endpoints, and run new transactions / workflows)
 */
export interface DBOSExecutorEventReceiverInterface
{
  readonly logger: Logger;
  readonly tracer: Tracer;

  /*
   * Get the registrations for a receiver; this comes with:
   *  minfo: the method info the receiver stored
   *  cinfo: the class info the receiver stored
   *  method: the method registration (w/ workflow, transaction, function, and other info)
   */
  getRegistrationsFor(eri: DBOSEventReceiver) : {minfo: unknown, cinfo: unknown, method: MethodRegistrationBase}[];

  transaction<T extends unknown[], R>(txn: Transaction<T, R>, params: WorkflowParams, ...args: T): Promise<R>;
  workflow<T extends unknown[], R>(wf: WorkflowFunction<T, R>, params: WorkflowParams, ...args: T): Promise<WorkflowHandle<R>>;
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
    destroy() : Promise<void>;
    initialize(executor: DBOSExecutorEventReceiverInterface) : Promise<void>;
    logRegisteredEndpoints() : void;
}