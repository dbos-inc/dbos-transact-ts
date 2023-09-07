/* eslint-disable @typescript-eslint/no-explicit-any */
// TODO: should we support log function in handler?
import { OperonMethodRegistration, OperonParameter, registerAndWrapFunction } from "../decorators";
import { OperonContext } from "../context";
import { Operon } from "../operon";
import Koa from 'koa';
import { getOrCreateOperonMethodArgsRegistration } from "src/decorators";

export class HandlerContext extends OperonContext {
  constructor(readonly operon: Operon,
    readonly koaContext: Koa.Context) {
    super();
  }
}

export enum APITypes {
  GET = 'GET',
  POST = 'POST'
}

export enum ArgSources {
  DEFAULT = 'DEFAULT',
  BODY = 'BODY',
  QUERY = 'QUERY',
  URL = 'URL'
}

export class OperonHandlerRegistration <This, Args extends unknown[], Return>
extends OperonMethodRegistration<This, Args, Return> {
  apiType: APITypes = APITypes.GET;
  apiURL: string = '';

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
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>) {
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
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>) {
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

