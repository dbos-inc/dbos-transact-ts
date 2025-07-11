import { DBOS } from '@dbos-inc/dbos-sdk';
import Koa from 'koa';
import Router from '@koa/router';
import { DBOSKoa } from '../src';

import request from 'supertest';

const dhttp = new DBOSKoa();

function customstep() {
  return function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: PropertyKey,
    descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  ) {
    if (!descriptor.value) {
      throw Error('Use of decorator when original method is undefined');
    }

    descriptor.value = DBOS.registerStep(descriptor.value, {
      name: String(propertyKey),
      ctorOrProto: target,
    });

    return descriptor;
  };
}

describe('registerstep', () => {
  let app: Koa;
  let appRouter: Router;

  beforeAll(async () => {});

  afterAll(async () => {});

  beforeEach(async () => {
    DBOS.setConfig({ name: 'koa-step-test' });
    await DBOS.launch();
    app = new Koa();
    appRouter = new Router();
    dhttp.registerWithApp(app, appRouter);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('use-customstep-and-koa', async () => {
    const u1 = await KnexKoa.insertOneWay('joe');
    expect(u1.user).toBe('joe');
    const u2 = await KnexKoa.insertTheOtherWay('jack');
    expect(u2.user).toBe('jack');

    const response1 = await request(app.callback()).get('/api/i1?user=john');
    expect(response1.statusCode).toBe(200);
    const response2 = await request(app.callback()).get('/api/i2?user=jeremy');
    expect(response2.statusCode).toBe(200);
    const response3 = await request(app.callback()).get('/api/i1');
    expect(response3.statusCode).toBe(400);
    const response4 = await request(app.callback()).get('/api/i2');
    expect(response4.statusCode).toBe(400);
  });
});

@DBOSKoa.defaultArgValidate
class KnexKoa {
  @customstep()
  @dhttp.getApi('/api/i2')
  static async insertTheOtherWay(user: string) {
    return Promise.resolve({ user, now: Date.now() });
  }

  @dhttp.getApi('/api/i1')
  @customstep()
  static async insertOneWay(user: string) {
    return Promise.resolve({ user, now: Date.now() });
  }
}
