import {
  MethodParameter,
  registerAndWrapFunctionTakingContext,
  getOrCreateMethodArgsRegistration,
  MethodRegistrationBase,
  getRegisteredOperations,
  ConfiguredInstance,
} from '../decorators';
import { DBOSExecutor, OperationType } from '../dbos-executor';
import { DBOSContext, DBOSContextImpl } from '../context';
import Koa from 'koa';
import {
  Workflow,
  TailParameters,
  WorkflowHandle,
  WorkflowParams,
  WorkflowContext,
  WFInvokeFuncs,
  WFInvokeFuncsInst,
  GetWorkflowsInput,
  GetWorkflowsOutput,
} from '../workflow';
import { Transaction } from '../transaction';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { trace, defaultTextMapGetter, ROOT_CONTEXT } from '@opentelemetry/api';
import { Span } from '@opentelemetry/sdk-trace-base';
import { StepFunction } from '../step';
import { APITypes, ArgSources } from './handlerTypes';
import { StoredProcedure } from '../procedure';
import { WorkflowQueue } from '../wfqueue';
import { getOrGenerateRequestID, RequestIDHeader } from './middleware';

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

// TODO: this should be refactored to not take a koaContext in.
export class HandlerContextImpl extends DBOSContextImpl implements HandlerContext {
  readonly #dbosExec: DBOSExecutor;
  readonly W3CTraceContextPropagator: W3CTraceContextPropagator;

  constructor(
    dbosExec: DBOSExecutor,
    readonly koaContext: Koa.Context,
  ) {
    // Retrieve or generate the request ID
    const requestID = getOrGenerateRequestID(koaContext.request.headers);
    koaContext.set(RequestIDHeader, requestID);

    // If present, retrieve the trace context from the request
    const httpTracer = new W3CTraceContextPropagator();
    const extractedSpanContext = trace.getSpanContext(
      httpTracer.extract(ROOT_CONTEXT, koaContext.request.headers, defaultTextMapGetter),
    );
    let span: Span;
    const spanAttributes = {
      operationType: OperationType.HANDLER,
      requestID: requestID,
      requestIP: koaContext.request.ip,
      requestURL: koaContext.request.url,
      requestMethod: koaContext.request.method,
    };
    if (extractedSpanContext === undefined) {
      span = dbosExec.tracer.startSpan(koaContext.url, spanAttributes);
    } else {
      extractedSpanContext.isRemote = true;
      span = dbosExec.tracer.startSpanWithContext(extractedSpanContext, koaContext.url, spanAttributes);
    }

    super(koaContext.url, span, dbosExec.logger);

    // If running in DBOS Cloud, set the executor ID
    if (process.env.DBOS__VMID) {
      this.executorID = process.env.DBOS__VMID;
    }

    this.W3CTraceContextPropagator = httpTracer;
    this.request = {
      headers: koaContext.request.headers,
      rawHeaders: koaContext.req.rawHeaders,
      params: koaContext.params,
      body: koaContext.request.body,
      rawBody: koaContext.request.rawBody,
      query: koaContext.request.query,
      querystring: koaContext.request.querystring,
      url: koaContext.request.url,
      ip: koaContext.request.ip,
      requestID: requestID,
    };
    this.applicationConfig = dbosExec.config.application;
    this.#dbosExec = dbosExec;
  }

  ///////////////////////
  /* PUBLIC INTERFACE  */
  ///////////////////////

  async send<T>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    return this.#dbosExec.send(destinationUUID, message, topic, idempotencyKey);
  }

  async getEvent<T>(
    workflowUUID: string,
    key: string,
    timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec,
  ): Promise<T | null> {
    return this.#dbosExec.getEvent(workflowUUID, key, timeoutSeconds);
  }

  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R> {
    return this.#dbosExec.retrieveWorkflow(workflowUUID);
  }

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to use WorkflowContext.Transaction(OpClass.someMethod, param);
   */
  mainInvoke<T extends object>(
    object: T,
    workflowUUID: string | undefined,
    asyncWf: boolean,
    configuredInstance: ConfiguredInstance | null,
    queue?: WorkflowQueue,
  ): InvokeFuncs<T> {
    const ops = getRegisteredOperations(object);
    const proxy: Record<string, unknown> = {};
    const params = { workflowUUID: workflowUUID, parentCtx: this, configuredInstance, queueName: queue?.name };

    for (const op of ops) {
      if (asyncWf) {
        proxy[op.name] = op.txnConfig
          ? (...args: unknown[]) =>
              this.#transaction(op.registeredFunction as Transaction<unknown[], unknown>, params, ...args)
          : op.workflowConfig
            ? (...args: unknown[]) =>
                this.#workflow(op.registeredFunction as Workflow<unknown[], unknown>, params, ...args)
            : op.stepConfig
              ? (...args: unknown[]) =>
                  this.#external(op.registeredFunction as StepFunction<unknown[], unknown>, params, ...args)
              : op.procConfig
                ? (...args: unknown[]) =>
                    this.#procedure(op.registeredFunction as StoredProcedure<unknown[], unknown>, params, ...args)
                : undefined;
      } else {
        proxy[op.name] = op.workflowConfig
          ? (...args: unknown[]) =>
              this.#workflow(op.registeredFunction as Workflow<unknown[], unknown>, params, ...args).then((handle) =>
                handle.getResult(),
              )
          : undefined;
      }
    }
    return proxy as InvokeFuncs<T>;
  }

  invoke<T extends object>(object: T | ConfiguredInstance, workflowUUID?: string): InvokeFuncs<T> | InvokeFuncsInst<T> {
    if (typeof object === 'function') {
      return this.mainInvoke(object, workflowUUID, true, null, undefined);
    } else {
      const targetInst = object as ConfiguredInstance;
      return this.mainInvoke(targetInst, workflowUUID, true, targetInst, undefined) as unknown as InvokeFuncsInst<T>;
    }
  }

  startWorkflow<T extends object>(
    object: T | ConfiguredInstance,
    workflowUUID?: string,
    queue?: WorkflowQueue,
  ): AsyncHandlerWfFuncs<T> | AsyncHandlerWfFuncInst<T> {
    if (typeof object === 'function') {
      return this.mainInvoke(object, workflowUUID, true, null, queue);
    } else {
      const targetInst = object as ConfiguredInstance;
      return this.mainInvoke(targetInst, workflowUUID, true, targetInst, queue) as unknown as AsyncHandlerWfFuncInst<T>;
    }
  }

  invokeWorkflow<T extends object>(
    object: T | ConfiguredInstance,
    workflowUUID?: string,
  ): SyncHandlerWfFuncs<T> | SyncHandlerWfFuncsInst<T> {
    if (typeof object === 'function') {
      return this.mainInvoke(object, workflowUUID, false, null, undefined) as unknown as SyncHandlerWfFuncs<T>;
    } else {
      const targetInst = object as ConfiguredInstance;
      return this.mainInvoke(
        targetInst,
        workflowUUID,
        false,
        targetInst,
        undefined,
      ) as unknown as SyncHandlerWfFuncsInst<T>;
    }
  }

  async getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput> {
    const wfs = await this.#dbosExec.listWorkflows(input);
    return { workflowUUIDs: wfs.map((wf) => wf.workflowID) };
  }

  //////////////////////
  /* PRIVATE METHODS */
  /////////////////////

  async #workflow<T extends unknown[], R>(
    wf: Workflow<T, R>,
    params: WorkflowParams,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    return this.#dbosExec.workflow(wf, params, ...args);
  }

  async #transaction<T extends unknown[], R>(txn: Transaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    return this.#dbosExec.transaction(txn, params, ...args);
  }

  async #external<T extends unknown[], R>(stepFn: StepFunction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    return this.#dbosExec.external(stepFn, params, ...args);
  }

  async #procedure<T extends unknown[], R>(
    proc: StoredProcedure<T, R>,
    params: WorkflowParams,
    ...args: T
  ): Promise<R> {
    return this.#dbosExec.procedure(proc, params, ...args);
  }
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

/////////////////////////
/* ENDPOINT DECORATORS */
/////////////////////////

function generateApiDec(verb: APITypes, url: string) {
  return function apidec<This, Ctx extends DBOSContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>,
  ) {
    const { descriptor, registration } = registerAndWrapFunctionTakingContext(target, propertyKey, inDescriptor);
    const handlerRegistration = registration as unknown as HandlerRegistrationBase;
    handlerRegistration.apiURL = url;
    handlerRegistration.apiType = verb;
    registration.performArgValidation = true;

    return descriptor;
  };
}

/** @deprecated Use `DBOS.getApi` */
export function GetApi(url: string) {
  return generateApiDec(APITypes.GET, url);
}

/** @deprecated  Use `DBOS.postApi` */
export function PostApi(url: string) {
  return generateApiDec(APITypes.POST, url);
}

/** @deprecated  Use `DBOS.putApi` */
export function PutApi(url: string) {
  return generateApiDec(APITypes.PUT, url);
}

/** @deprecated  Use `DBOS.patchApi` */
export function PatchApi(url: string) {
  return generateApiDec(APITypes.PATCH, url);
}

/** @deprecated  Use `DBOS.deleteApi` */
export function DeleteApi(url: string) {
  return generateApiDec(APITypes.DELETE, url);
}

///////////////////////////////////
/* ENDPOINT PARAMETER DECORATORS */
///////////////////////////////////

export function ArgSource(source: ArgSources) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex] as HandlerParameter;
    curParam.argSource = source;
  };
}
