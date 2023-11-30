/* eslint-disable @typescript-eslint/no-explicit-any */
import { MethodRegistration, MethodParameter, registerAndWrapFunction, getOrCreateMethodArgsRegistration, MethodRegistrationBase, getRegisteredOperations } from "../decorators";
import { DBOSExecutor } from "../dbos-executor";
import { DBOSContext, DBOSContextImpl } from "../context";
import Koa from "koa";
import { DBOSWorkflow, TailParameters, WorkflowHandle, WorkflowParams, WorkflowContext, WFInvokeFuncs } from "../workflow";
import { DBOSTransaction } from "../transaction";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { trace, defaultTextMapGetter, ROOT_CONTEXT } from '@opentelemetry/api';
import { Span } from "@opentelemetry/sdk-trace-base";
import { DBOSCommunicator } from "../communicator";

// local type declarations for workflow functions
type WFFunc = (ctxt: WorkflowContext, ...args: any[]) => Promise<any>;
export type InvokeFuncs<T> = WFInvokeFuncs<T> & HandlerWfFuncs<T>;

type HandlerWfFuncs<T> = {
  [P in keyof T as T[P] extends WFFunc ? P : never]: T[P] extends WFFunc ? (...args: TailParameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>> : never;
}

export interface HandlerContext extends DBOSContext {
  readonly koaContext: Koa.Context;
  invoke<T extends object>(targetClass: T, workflowUUID?: string): InvokeFuncs<T>;
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;
  send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void>;
  getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds?: number): Promise<T | null>;
}

export class HandlerContextImpl extends DBOSContextImpl implements HandlerContext {
  readonly #wfe: DBOSExecutor;
  readonly W3CTraceContextPropagator: W3CTraceContextPropagator;

  constructor(wfe: DBOSExecutor, readonly koaContext: Koa.Context) {
    // If present, retrieve the trace context from the request
    const httpTracer = new W3CTraceContextPropagator();
    const extractedSpanContext = trace.getSpanContext(
        httpTracer.extract(ROOT_CONTEXT, koaContext.request.headers, defaultTextMapGetter)
    )
    let span: Span;
    const spanAttributes = {
      operationName: koaContext.url,
    };
    if (extractedSpanContext === undefined) {
      span = wfe.tracer.startSpan(koaContext.url, spanAttributes);
    } else {
      extractedSpanContext.isRemote = true;
      span = wfe.tracer.startSpanWithContext(extractedSpanContext, koaContext.url, spanAttributes);
    }
    super(koaContext.url, span, wfe.logger);
    this.W3CTraceContextPropagator = httpTracer;
    this.request = {
      headers: koaContext.request.headers,
      rawHeaders: koaContext.req.rawHeaders,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      params: koaContext.params,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      body: koaContext.request.body,
      rawBody: koaContext.request.rawBody,
      query: koaContext.request.query,
      querystring: koaContext.request.querystring,
      url: koaContext.request.url,
      ip: koaContext.request.ip,
    };
    if (wfe.config.application) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.applicationConfig = wfe.config.application;
    }
    this.#wfe = wfe;
  }

  ///////////////////////
  /* PUBLIC INTERFACE  */
  ///////////////////////

  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    return this.#wfe.send(destinationUUID, message, topic, idempotencyKey);
  }

  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec): Promise<T | null> {
    return this.#wfe.getEvent(workflowUUID, key, timeoutSeconds);
  }

  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R> {
    return this.#wfe.retrieveWorkflow(workflowUUID);
  }

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to use WorkflowContext.Transaction(OpClass.someMethod, param);
   */
  invoke<T extends object>(object: T, workflowUUID?: string): InvokeFuncs<T> {
    const ops = getRegisteredOperations(object);

    const proxy: any = {};
    const params = { workflowUUID: workflowUUID, parentCtx: this };
    for (const op of ops) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      proxy[op.name] = op.txnConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.#transaction(op.registeredFunction as DBOSTransaction<any[], any>, params, ...args)
        : op.workflowConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.#workflow(op.registeredFunction as DBOSWorkflow<any[], any>, params, ...args)
        : op.commConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.#external(op.registeredFunction as DBOSCommunicator<any[], any>, params, ...args)
        : undefined;
    }
    return proxy as InvokeFuncs<T>;
  }

  //////////////////////
  /* PRIVATE METHODS */
  /////////////////////

  async #workflow<T extends any[], R>(wf: DBOSWorkflow<T, R>, params: WorkflowParams, ...args: T): Promise<WorkflowHandle<R>> {
    return this.#wfe.workflow(wf, params, ...args);
  }

  async #transaction<T extends any[], R>(txn: DBOSTransaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    return this.#wfe.transaction(txn, params, ...args);
  }

  async #external<T extends any[], R>(commFn: DBOSCommunicator<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    return this.#wfe.external(commFn, params, ...args);
  }
}

//////////////////////////
/* REGISTRATION OBJECTS */
//////////////////////////

export enum APITypes {
  GET = "GET",
  POST = "POST",
}

export enum ArgSources {
  DEFAULT = "DEFAULT",
  BODY = "BODY",
  QUERY = "QUERY",
  URL = "URL",
}

export interface HandlerRegistrationBase extends MethodRegistrationBase {
  apiType: APITypes;
  apiURL: string;
  args: HandlerParameter[];
}

export class HandlerRegistration<This, Args extends unknown[], Return> extends MethodRegistration<This, Args, Return> {
  apiType: APITypes = APITypes.GET;
  apiURL: string = "";

  args: HandlerParameter[] = [];
  constructor(origFunc: (this: This, ...args: Args) => Promise<Return>) {
    super(origFunc);
  }
}

export class HandlerParameter extends MethodParameter {
  argSource: ArgSources = ArgSources.DEFAULT;

  // eslint-disable-next-line @typescript-eslint/ban-types
  constructor(idx: number, at: Function) {
    super(idx, at);
  }
}

/////////////////////////
/* ENDPOINT DECORATORS */
/////////////////////////

export function GetApi(url: string) {
  function apidec<This, Ctx extends DBOSContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>
  ) {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    const handlerRegistration = registration as unknown as HandlerRegistration<This, Args, Return>;
    handlerRegistration.apiURL = url;
    handlerRegistration.apiType = APITypes.GET;

    return descriptor;
  }
  return apidec;
}

export function PostApi(url: string) {
  function apidec<This, Ctx extends DBOSContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>
  ) {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    const handlerRegistration = registration as unknown as HandlerRegistration<This, Args, Return>;
    handlerRegistration.apiURL = url;
    handlerRegistration.apiType = APITypes.POST;

    return descriptor;
  }
  return apidec;
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
