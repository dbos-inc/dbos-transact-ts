/* eslint-disable @typescript-eslint/no-explicit-any */
import { OperonMethodRegistration, OperonParameter, registerAndWrapFunction, getOrCreateOperonMethodArgsRegistration, OperonMethodRegistrationBase } from "../decorators";
import { OperonContext } from "../context";
import { InternalWorkflowParams, Operon } from "../operon";
import Koa from "koa";
import { OperonWorkflow, WorkflowContext, WorkflowHandle, WorkflowParams } from "../workflow";
import { OperonTransaction } from "../transaction";

export class HandlerContext extends OperonContext {
  readonly #operon: Operon;

  constructor(operon: Operon, readonly koaContext: Koa.Context) {
    const span = operon.tracer.startSpan(koaContext.url);
    span.setAttributes({
      operationName: koaContext.url,
    });
    super(koaContext.url, span);
    this.request = koaContext.req;
    if (operon.config.application) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.applicationConfig = operon.config.application;
    }
    this.#operon = operon;
  }

  log(severity: string, message: string): void {
    // TODO: need to clean up the logging interface.
    // `log` expects workflowUUID and other fields to be set so we need to convert.
    this.#operon.logger.log(this as unknown as WorkflowContext, severity, message);
  }

  workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T): WorkflowHandle<R> {
    const augmentedParams = params as InternalWorkflowParams;
    augmentedParams.parentCtx = this;
    return this.#operon.workflow(wf, augmentedParams, ...args);
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    return this.#operon.transaction(txn, params, ...args);
  }

  async send<T extends NonNullable<any>>(params: WorkflowParams, destinationUUID: string, message: T, topic: string): Promise<void> {
    return this.#operon.send(params, destinationUUID, message, topic);
  }

  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number = 60): Promise<T | null> {
    return this.#operon.getEvent(workflowUUID, key, timeoutSeconds);
  }

  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R> {
    return this.#operon.retrieveWorkflow(workflowUUID);
  }
}

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

export interface OperonHandlerRegistrationBase extends OperonMethodRegistrationBase
{
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

export function ArgSource(source: ArgSources) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex] as OperonHandlerParameter;
    curParam.argSource = source;
  };
}
