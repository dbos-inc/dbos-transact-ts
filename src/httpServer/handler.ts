/* eslint-disable @typescript-eslint/no-explicit-any */
// TODO: should we support log function in handler?
import { OperonContext } from "../context";
import { Operon } from "../operon";
import Koa from "koa";

export class HandlerContext extends OperonContext {
  constructor(readonly operon: Operon, readonly koaContext: Koa.Context) {
    super();
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

// export class OperonHandlerRegistration<This, Args extends unknown[], Return> extends OperonMethodRegistration<This, Args, Return> {
//   apiType: APITypes = APITypes.GET;
//   apiURL: string = "";

//   args: OperonHandlerParameter[] = [];
//   constructor(origFunc: (this: This, ...args: Args) => Promise<Return>) {
//     super(origFunc);
//   }
// }

// export class OperonHandlerParameter extends OperonParameter {
//   argSource: ArgSources = ArgSources.DEFAULT;

//   // eslint-disable-next-line @typescript-eslint/ban-types
//   constructor(idx: number, at: Function) {
//     super(idx, at);
//   }
// }

export function GetApi(url: string) {
  function apidec<This, Ctx extends OperonContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    _inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>
  ) {
    Reflect.defineMetadata("operon:http-server", { type: APITypes.GET, url}, target, propertyKey);
  }
  return apidec;
}

export function PostApi(url: string) {
  return function <This, Ctx extends OperonContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    _inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>
  ) {
    Reflect.defineMetadata("operon:http-server", { type: APITypes.POST, url}, target, propertyKey);
  }
}

export function ArgSource(source: ArgSources) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const params = Reflect.getOwnMetadata("operon:http-arg-source", target, propertyKey) as Array<ArgSources> ?? [];
    params[parameterIndex] = source;
  };
}
