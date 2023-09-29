/* eslint-disable @typescript-eslint/no-explicit-any */
import { OperonMethodRegistration, OperonParameter, registerAndWrapFunction, getOrCreateOperonMethodArgsRegistration, OperonMethodRegistrationBase, getRegisteredOperations } from "../decorators";
import { OperonContext } from "../context";
import { Operon } from "../operon";
import Koa from "koa";
import { OperonWorkflow, TailParameters, WorkflowContext, WorkflowHandle, WorkflowParams } from "../workflow";
import { OperonTransaction, TransactionContext } from "../transaction";

type TxFunc = (ctxt: TransactionContext, ...args: any[]) => Promise<any>;
type WFFunc = (ctxt: WorkflowContext, ...args: any[]) => Promise<any>;

// Utility type that only includes operon transaction/communicator functions + converts the method signature to exclude the context parameter
type HandlerTxFuncs<T> = {
  [P in keyof T as T[P] extends TxFunc ? P : never]: T[P] extends TxFunc ? (...args: TailParameters<T[P]>) => ReturnType<T[P]> : never;
}

type HandlerWfFuncs<T> = {
  [P in keyof T as T[P] extends WFFunc ? P : never]: T[P] extends WFFunc ? (...args: TailParameters<T[P]>) => WorkflowHandle<Awaited<ReturnType<T[P]>>> : never;
}

export class HandlerContext extends OperonContext {
  readonly #operon: Operon;

  constructor(operon: Operon, readonly koaContext: Koa.Context) {
    const span = operon.tracer.startSpan(koaContext.url);
    span.setAttributes({
      operationName: koaContext.url,
    });
    super(koaContext.url, span, operon.logger);
    this.request = koaContext.req;
    if (operon.config.application) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.applicationConfig = operon.config.application;
    }
    this.#operon = operon;
  }

  ///////////////////////
  /* PUBLIC INTERFACE  */
  ///////////////////////

  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic: string): Promise<void> {
    return this.#operon.send({}, destinationUUID, message, topic);
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
  invoke<T extends object>(object: T, workflowUUID?: string): HandlerTxFuncs<T> & HandlerWfFuncs<T> {
    const ops = getRegisteredOperations(object);

    const proxy: any = {};
    const params = workflowUUID ? { workflowUUID: workflowUUID } : {};
    for (const op of ops) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      proxy[op.name] = op.txnConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.transaction(op.registeredFunction as OperonTransaction<any[], any>, params, ...args)
        : op.workflowConfig
          // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
          ? (...args: any[]) => this.workflow(op.registeredFunction as OperonWorkflow<any[], any>, params, ...args)
          : undefined;
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return proxy;
  }

  //////////////////////
  /* PRIVATE METHODS */
  /////////////////////

  // TODO: Make private
  workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T): WorkflowHandle<R> {
    params.parentCtx = this;
    return this.#operon.workflow(wf, params, ...args);
  }

  // TODO: Make private
  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    return this.#operon.transaction(txn, params, ...args);
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
