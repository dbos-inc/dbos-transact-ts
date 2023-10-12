/* eslint-disable @typescript-eslint/no-explicit-any */
import { OperonMethodRegistration, OperonParameter, registerAndWrapFunction, getOrCreateOperonMethodArgsRegistration, OperonMethodRegistrationBase, getRegisteredOperations } from "../decorators";
import { Operon } from "../operon";
import { OperonContext, OperonContextImpl } from "../context";
import Koa from "koa";
import { OperonWorkflow, TailParameters, WorkflowHandle, WorkflowParams, WorkflowContext, WFInvokeFuncs } from "../workflow";
import { OperonTransaction } from "../transaction";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { trace, defaultTextMapGetter, ROOT_CONTEXT } from '@opentelemetry/api';
import { Span } from "@opentelemetry/sdk-trace-base";
import { OperonCommunicator } from "../communicator";

// local type declarations for Operon workflow functions
type WFFunc = (ctxt: WorkflowContext, ...args: any[]) => Promise<any>;
export type InvokeFuncs<T> = WFInvokeFuncs<T> & HandlerWfFuncs<T>;

type HandlerWfFuncs<T> = {
  [P in keyof T as T[P] extends WFFunc ? P : never]: T[P] extends WFFunc ? (...args: TailParameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>> : never;
}

export interface HandlerContext extends OperonContext {
  readonly koaContext: Koa.Context;
  invoke<T extends object>(targetClass: T, workflowUUID?: string): InvokeFuncs<T>;
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;
  send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void>;
  getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds?: number): Promise<T | null>;
}

export class HandlerContextImpl extends OperonContextImpl implements HandlerContext {
  readonly #operon: Operon;
  readonly W3CTraceContextPropagator: W3CTraceContextPropagator;

  constructor(operon: Operon, readonly koaContext: Koa.Context) {
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
      span = operon.tracer.startSpan(koaContext.url, spanAttributes);
    } else {
      extractedSpanContext.isRemote = true;
      span = operon.tracer.startSpanWithContext(extractedSpanContext, koaContext.url, spanAttributes);
    }
    super(koaContext.url, span, operon.logger);
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
    if (operon.config.application) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.applicationConfig = operon.config.application;
    }
    this.#operon = operon;
  }

  ///////////////////////
  /* PUBLIC INTERFACE  */
  ///////////////////////

  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    return this.#operon.send(destinationUUID, message, topic, idempotencyKey);
  }

  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number = 60): Promise<T | null> {
    return this.#operon.getEvent(workflowUUID, key, timeoutSeconds);
  }

  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R> {
    return this.#operon.retrieveWorkflow(workflowUUID);
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
        ? (...args: any[]) => this.#transaction(op.registeredFunction as OperonTransaction<any[], any>, params, ...args)
        : op.workflowConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.#workflow(op.registeredFunction as OperonWorkflow<any[], any>, params, ...args)
        : op.commConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.#external(op.registeredFunction as OperonCommunicator<any[], any>, params, ...args)
        : undefined;
    }
    return proxy as InvokeFuncs<T>;
  }

  //////////////////////
  /* PRIVATE METHODS */
  /////////////////////

  async #workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T): Promise<WorkflowHandle<R>> {
    return this.#operon.workflow(wf, params, ...args);
  }

  async #transaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    return this.#operon.transaction(txn, params, ...args);
  }

  async #external<T extends any[], R>(commFn: OperonCommunicator<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    return this.#operon.external(commFn, params, ...args);
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

export interface OperonHandlerRegistrationBase extends OperonMethodRegistrationBase {
  apiType: APITypes;
  apiURL: string;
  args: OperonHandlerParameter[];
}

export class OperonHandlerRegistration<This, Args extends unknown[], Return> extends OperonMethodRegistration<This, Args, Return> {
  apiType: APITypes = APITypes.GET;
  apiURL: string = "";

  args: OperonHandlerParameter[] = [];
  constructor(origFunc: (this: This, ...args: Args) => Promise<Return>) {
    super(origFunc);
  }
}

export class OperonHandlerParameter extends OperonParameter {
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
  function apidec<This, Ctx extends OperonContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>
  ) {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    const handlerRegistration = registration as unknown as OperonHandlerRegistration<This, Args, Return>;
    handlerRegistration.apiURL = url;
    handlerRegistration.apiType = APITypes.GET;

    return descriptor;
  }
  return apidec;
}

export function PostApi(url: string) {
  function apidec<This, Ctx extends OperonContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>
  ) {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    const handlerRegistration = registration as unknown as OperonHandlerRegistration<This, Args, Return>;
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
    const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex] as OperonHandlerParameter;
    curParam.argSource = source;
  };
}
