import {
  MethodParameter,
  getOrCreateMethodArgsRegistration,
  MethodRegistrationBase,
  ConfiguredInstance,
} from '../decorators';
import { DBOSContext } from '../context';
import Koa from 'koa';
import {
  TailParameters,
  WorkflowHandle,
  WorkflowContext,
  WFInvokeFuncs,
  WFInvokeFuncsInst,
  GetWorkflowsInput,
  GetWorkflowsOutput,
} from '../workflow';
import { APITypes, ArgSources } from './handlerTypes';
import { WorkflowQueue } from '../wfqueue';

// local type declarations for workflow functions
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type WFFunc = (ctxt: WorkflowContext, ...args: any[]) => Promise<unknown>;
export type InvokeFuncs<T> = WFInvokeFuncs<T> & AsyncHandlerWfFuncs<T>;
export type InvokeFuncsInst<T> = WFInvokeFuncsInst<T>;

export type AsyncHandlerWfFuncs<T> = T extends ConfiguredInstance
  ? never
  : {
      [P in keyof T as T[P] extends WFFunc ? P : never]: T[P] extends WFFunc
        ? (...args: TailParameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>>
        : never;
    };

export type SyncHandlerWfFuncs<T> = T extends ConfiguredInstance
  ? never
  : {
      [P in keyof T as T[P] extends WFFunc ? P : never]: T[P] extends WFFunc
        ? (...args: TailParameters<T[P]>) => Promise<Awaited<ReturnType<T[P]>>>
        : never;
    };

export type AsyncHandlerWfFuncInst<T> = T extends ConfiguredInstance
  ? {
      [P in keyof T as T[P] extends WFFunc ? P : never]: T[P] extends WFFunc
        ? (...args: TailParameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>>
        : never;
    }
  : never;

export type SyncHandlerWfFuncsInst<T> = T extends ConfiguredInstance
  ? {
      [P in keyof T as T[P] extends WFFunc ? P : never]: T[P] extends WFFunc
        ? (...args: TailParameters<T[P]>) => Promise<Awaited<ReturnType<T[P]>>>
        : never;
    }
  : never;

/**
 * @deprecated This class is no longer necessary
 * To update to Transact 2.0+
 *   Remove `HandlerContext` from function parameter lists
 *   Use `@DBOS.getApi`, `@DBOS.postApi`, etc., decorators instead of `@GetApi`, `@PostApi`, etc.
 *   Use `DBOS.` to access DBOS context within affected functions
 */
export interface HandlerContext extends DBOSContext {
  readonly koaContext: Koa.Context;
  invoke<T extends ConfiguredInstance>(targetCfg: T, workflowUUID?: string): InvokeFuncsInst<T>;
  invoke<T extends object>(targetClass: T, workflowUUID?: string): InvokeFuncs<T>;
  invokeWorkflow<T extends ConfiguredInstance>(targetCfg: T, workflowUUID?: string): SyncHandlerWfFuncsInst<T>;
  invokeWorkflow<T extends object>(targetClass: T, workflowUUID?: string): SyncHandlerWfFuncs<T>;
  startWorkflow<T extends ConfiguredInstance>(
    targetCfg: T,
    workflowUUID?: string,
    queue?: WorkflowQueue,
  ): AsyncHandlerWfFuncInst<T>;
  startWorkflow<T extends object>(targetClass: T, workflowUUID?: string, queue?: WorkflowQueue): AsyncHandlerWfFuncs<T>;

  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;
  send<T>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void>;
  getEvent<T>(workflowUUID: string, key: string, timeoutSeconds?: number): Promise<T | null>;
  getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput>;
}

export interface HandlerRegistrationBase extends MethodRegistrationBase {
  apiType: APITypes;
  apiURL: string;
  args: HandlerParameter[];
}

export class HandlerParameter extends MethodParameter {
  argSource: ArgSources = ArgSources.DEFAULT;

  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  constructor(idx: number, at: Function) {
    super(idx, at);
  }
}

///////////////////////////////////
/* ENDPOINT PARAMETER DECORATORS */
///////////////////////////////////

export function ArgSource(source: ArgSources) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateMethodArgsRegistration(target, undefined, propertyKey);

    const curParam = existingParameters[parameterIndex] as HandlerParameter;
    curParam.argSource = source;
  };
}
