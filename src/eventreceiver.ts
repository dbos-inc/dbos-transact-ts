import { Tracer } from './telemetry/traces';
import { GlobalLogger as Logger } from './telemetry/logs';
import { Workflow, WorkflowHandle, WorkflowParams } from './workflow';
import { Transaction } from './transaction';
import { MethodRegistrationBase } from './decorators';

export interface DBOSExecutorPollerInterface
{
  readonly logger: Logger;
  readonly tracer: Tracer;

  // TODO We may make this better...
  getRegistrationsFor(eri: DBOSEventReceiver) : MethodRegistrationBase[];

  transaction<T extends unknown[], R>(txn: Transaction<T, R>, params: WorkflowParams, ...args: T): Promise<R>;
  workflow<T extends unknown[], R>(wf: Workflow<T, R>, params: WorkflowParams, ...args: T): Promise<WorkflowHandle<R>>;
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
    initialize(executor: DBOSExecutorPollerInterface) : Promise<void>;
    logRegisteredEndpoints() : void;
}