import Koa from 'koa';
import Router from '@koa/router';

import { DBOS } from '@dbos-inc/dbos-sdk';

import { DBOSKoa } from '../src';

import request from 'supertest';

const dhttp = new DBOSKoa();

let middlewareCounter = 0;
const testMiddleware: Koa.Middleware = async (_ctx, next) => {
  middlewareCounter++;
  await next();
};

let middlewareCounter2 = 0;
const testMiddleware2: Koa.Middleware = async (_ctx, next) => {
  middlewareCounter2 = middlewareCounter2 + 1;
  await next();
};

let middlewareCounterG = 0;
const testMiddlewareG: Koa.Middleware = async (_ctx, next) => {
  middlewareCounterG = middlewareCounterG + 1;
  expect(DBOS.logger).toBeDefined();
  await next();
};

@dhttp.koaGlobalMiddleware(testMiddlewareG)
@dhttp.koaGlobalMiddleware(testMiddleware, testMiddleware2)
export class HTTPEndpoints {
  @dhttp.getApi('/foobar')
  static async foobar(arg: string) {
    return Promise.resolve(`ARG: ${arg}`);
  }
}

describe('decoratorless-api-tests', () => {
  let app: Koa;
  let appRouter: Router;

  beforeAll(async () => {
    DBOS.setConfig({
      name: 'dbos-koa-test',
    });
    return Promise.resolve();
  });

  beforeEach(async () => {
    middlewareCounter = middlewareCounter2 = middlewareCounterG = 0;
    DBOS.registerLifecycleCallback(dhttp);
    await DBOS.launch();
    DBOS.logRegisteredEndpoints();
    app = new Koa();
    appRouter = new Router();
    dhttp.registerWithApp(app, appRouter);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-functions', async () => {
    const response1 = await request(app.callback()).get('/foobar?arg=A');
    expect(response1.statusCode).toBe(200);
    expect(response1.text).toBe('ARG: A');
    expect(middlewareCounter).toBe(1);
    expect(middlewareCounter2).toBe(1);
    expect(middlewareCounterG).toBe(1);
  });
});
