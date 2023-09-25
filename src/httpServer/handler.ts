/* eslint-disable @typescript-eslint/no-explicit-any */
// TODO: should we support log function in handler?
import { OperonMethodRegistration, OperonParameter, registerAndWrapFunction, getOrCreateOperonMethodArgsRegistration, OperonMethodRegistrationBase } from "../decorators";
import { OperonContext } from "../context";
import { Operon } from "../operon";
import Koa from "koa";
import { WorkflowContext } from "src/workflow";

export class HandlerContext extends OperonContext {
  readonly operationName: string;  // This is the URL.
  readonly runAs: string = "HTTPDefaultRole"; // TODO: add auth later.

  // TODO: Need to decide the semantics for those fields.
  readonly workflowUUID: string = 'N/A';
  readonly functionID: number = -1;

  constructor(readonly operon: Operon, readonly koaContext: Koa.Context) {
    super();
    this.operationName = koaContext.url;
    this.request = koaContext.req;
    if (operon.config.application) {
      this.applicationConfig = operon.config.application;
    }
  }

  log(severity: string, message: string): void {
    // TODO: need to clean up the logging interface.
    // `log` expects workflowUUID and other fields to be set so we need to convert.
    this.operon.logger.log(this as unknown as WorkflowContext, severity, message);
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
