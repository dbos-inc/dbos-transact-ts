import Koa from 'koa';
import Router from '@koa/router';

import { DBOS, Error as DBOSError } from '@dbos-inc/dbos-sdk';

import { DBOSKoa, DBOSKoaAuthContext } from '../src';

import request from 'supertest';

const dhttp = new DBOSKoa();

interface TestKvTable {
  id?: number;
  value?: string;
}

describe('httpserver-defsec-tests', () => {
  let app: Koa;
  let appRouter: Router;

  const testTableName = 'dbos_test_kv';

  beforeAll(async () => {
    DBOS.setConfig({
      name: 'dbos-koa-test',
      userDbclient: 'pg-node',
    });
    return Promise.resolve();
  });

  beforeEach(async () => {
    DBOS.registerLifecycleCallback(dhttp);
    const _classes = [TestEndpointDefSec, SecondClass];
    await DBOS.launch();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    middlewareCounter = 0;
    middlewareCounter2 = 0;
    middlewareCounterG = 0;
    app = new Koa();
    appRouter = new Router();
    dhttp.registerWithApp(app, appRouter);
  });

  afterEach(async () => {
    await DBOS.shutdown();
    jest.restoreAllMocks();
  });

  test('get-hello', async () => {
    const response = await request(app.callback()).get('/hello');
    expect(response.statusCode).toBe(200);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(response.body.message).toBe('hello!');
    expect(middlewareCounter).toBe(1);
    expect(middlewareCounter2).toBe(2); // Middleware runs from left to right.
    expect(middlewareCounterG).toBe(1);
    await request(app.callback()).get('/goodbye');
    expect(middlewareCounterG).toBe(2);
    await request(app.callback()).get('/nosuchendpoint');
    expect(middlewareCounterG).toBe(3);
  });

  test('get-hello-name', async () => {
    const response = await request(app.callback()).get('/hello/alice');
    expect(response.statusCode).toBe(200);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(response.body.message).toBe('hello, alice!');
    expect(middlewareCounter).toBe(1);
    expect(middlewareCounterG).toBe(1);
  });

  test('not-authenticated', async () => {
    const response = await request(app.callback()).get('/requireduser?name=alice');
    expect(response.statusCode).toBe(401);
  });

  test('not-you', async () => {
    const response = await request(app.callback()).get('/requireduser?name=alice&userid=go_away');
    expect(response.statusCode).toBe(401);
  });

  test('not-authorized', async () => {
    const response = await request(app.callback()).get('/requireduser?name=alice&userid=bob');
    expect(response.statusCode).toBe(403);
  });

  test('authorized', async () => {
    const response = await request(app.callback()).get('/requireduser?name=alice&userid=a_real_user');
    expect(response.statusCode).toBe(200);
  });

  // The handler is authorized, then its child workflow and transaction should also be authroized.
  test('cascade-authorized', async () => {
    const response = await request(app.callback()).get('/workflow?name=alice&userid=a_real_user');
    expect(response.statusCode).toBe(200);

    const txnResponse = await request(app.callback()).get('/transaction?name=alice&userid=a_real_user');
    expect(txnResponse.statusCode).toBe(200);
  });

  // We can directly test a transaction with passed in authorizedRoles.
  test('direct-transaction-test', async () => {
    await DBOS.withAuthedContext('user', ['user'], async () => {
      const res = await TestEndpointDefSec.testTranscation('alice');
      expect(res).toBe('hello 1');
    });

    // Unauthorized.
    await expect(TestEndpointDefSec.testTranscation('alice')).rejects.toThrow(
      new DBOSError.DBOSNotAuthorizedError('User does not have a role with permission to call testTranscation', 403),
    );
  });

  async function authTestMiddleware(ctx: DBOSKoaAuthContext) {
    if (ctx.requiredRole.length > 0) {
      const { userid } = ctx.koaContext.request.query;
      const uid = userid?.toString();

      if (!uid || uid.length === 0) {
        return Promise.reject(new DBOSError.DBOSNotAuthorizedError('Not logged in.', 401));
      } else {
        if (uid === 'go_away') {
          return Promise.reject(new DBOSError.DBOSNotAuthorizedError('Go away.', 401));
        }
        return Promise.resolve({
          authenticatedUser: uid,
          authenticatedRoles: uid === 'a_real_user' ? ['user'] : ['other'],
        });
      }
    }
    return;
  }

  let middlewareCounter = 0;
  const testMiddleware: Koa.Middleware = async (ctx, next) => {
    middlewareCounter++;
    await next();
  };

  let middlewareCounter2 = 0;
  const testMiddleware2: Koa.Middleware = async (ctx, next) => {
    middlewareCounter2 = middlewareCounter + 1;
    await next();
  };

  let middlewareCounterG = 0;
  const testMiddlewareG: Koa.Middleware = async (ctx, next) => {
    middlewareCounterG = middlewareCounterG + 1;
    await next();
  };

  @DBOS.defaultRequiredRole(['user'])
  @dhttp.authentication(authTestMiddleware)
  @dhttp.koaMiddleware(testMiddleware, testMiddleware2)
  @dhttp.koaGlobalMiddleware(testMiddlewareG)
  class TestEndpointDefSec {
    @DBOS.requiredRole([])
    @dhttp.getApi('/hello')
    static async hello() {
      return Promise.resolve({ message: 'hello!' });
    }

    @DBOS.requiredRole([])
    @dhttp.getApi('/hello/:name')
    static async helloName(name: string) {
      return Promise.resolve({ message: `hello, ${name}!` });
    }

    @dhttp.getApi('/requireduser')
    static async testAuth(name: string) {
      return Promise.resolve(`Please say hello to ${name}`);
    }

    @DBOS.transaction()
    static async testTranscation(name: string) {
      const { rows } = await DBOS.pgClient.query<TestKvTable>(
        `INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`,
        [name],
      );
      return `hello ${rows[0].id}`;
    }

    @DBOS.workflow()
    static async testWorkflow(name: string) {
      const res = await TestEndpointDefSec.testTranscation(name);
      return res;
    }

    @dhttp.getApi('/workflow')
    static async testWfEndpoint(name: string) {
      return await TestEndpointDefSec.testWorkflow(name);
    }

    @dhttp.getApi('/transaction')
    static async testTxnEndpoint(name: string) {
      return await TestEndpointDefSec.testTranscation(name);
    }
  }

  class SecondClass {
    @DBOS.requiredRole([])
    @dhttp.getApi('/goodbye')
    static async bye() {
      return Promise.resolve({ message: 'bye!' });
    }
  }
});
