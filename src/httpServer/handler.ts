/* eslint-disable @typescript-eslint/no-explicit-any */
// TODO: should we support log function in handler?
import { OperonContext } from "../context";
import { Operon } from "../operon";
import Koa from 'koa';

export class HandlerContext extends OperonContext {
  constructor(readonly operon: Operon,
    readonly koaContext: Koa.Context) {
    super();
  }
}