/* eslint-disable @typescript-eslint/no-explicit-any */
// TODO: should we support log function in handler?
import { OperonContext } from "../context";
import { Operon } from "../operon";
import Koa from "koa";

export class HandlerContext extends OperonContext {
  constructor(readonly operon: Operon, readonly koaContext: Koa.Context) {
    super();
    this.request = koaContext.request;
    this.response = koaContext.response;
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

export interface HttpEnpoint {
  type: APITypes, 
  url: string,
}

const operonHttpEndpointMetadataKey = Symbol("operon:http-endpoint");

export function getHttpEndpoint(target: object, propertyKey: string) {
  return Reflect.getOwnMetadata(operonHttpEndpointMetadataKey, target, propertyKey) as HttpEnpoint | undefined;

}

export function GetApi(url: string) {
  return function <This, Ctx extends OperonContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    _inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>
  ) {
    Reflect.defineMetadata(operonHttpEndpointMetadataKey, { type: APITypes.GET, url}, target, propertyKey);
  }
}

export function PostApi(url: string) {
  return function <This, Ctx extends OperonContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    _inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>
  ) {
    Reflect.defineMetadata(operonHttpEndpointMetadataKey, { type: APITypes.POST, url}, target, propertyKey);
  }
}

export function ArgSource(source: ArgSources) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const params = Reflect.getOwnMetadata("operon:http-arg-source", target, propertyKey) as Array<ArgSources> ?? [];
    if (params.length === 0) {
      Reflect.defineMetadata("operon:http-arg-source", params, target, propertyKey);
    }
    params[parameterIndex] = source;
  };
}
