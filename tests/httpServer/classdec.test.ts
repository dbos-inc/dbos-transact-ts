import {
  GetApi,
  RequiredRole,
  DefaultRequiredRole,
  MiddlewareContext,
  Transaction,
  Workflow,
  TransactionContext,
  WorkflowContext,
  DBOS,
} from '../../src';
import { TestKvTable, generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';
import request from 'supertest';
import { HandlerContext } from '../../src/httpServer/handler';
import { Authentication, KoaGlobalMiddleware, KoaMiddleware } from '../../src/httpServer/middleware';
import { Middleware } from 'koa';
import { DBOSNotAuthorizedError } from '../../src/error';
import { DBOSConfig } from '../../src/dbos-executor';
import { PoolClient } from 'pg';

describe('httpserver-defsec-tests', () => {
  const testTableName = 'dbos_test_kv';

  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    const _classes = [TestEndpointDefSec, SecondClass];
    await DBOS.launch();
    DBOS.setUpHandlerCallback();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    middlewareCounter = 0;
    middlewareCounter2 = 0;
    middlewareCounterG = 0;
  });

  afterEach(async () => {
    await DBOS.shutdown();
    jest.restoreAllMocks();
  });

  test('get-hello', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/hello');
    expect(response.statusCode).toBe(200);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(response.body.message).toBe('hello!');
    expect(middlewareCounter).toBe(1);
    expect(middlewareCounter2).toBe(2); // Middleware runs from left to right.
    expect(middlewareCounterG).toBe(1);
    await request(DBOS.getHTTPHandlersCallback()!).get('/goodbye');
    expect(middlewareCounterG).toBe(2);
    await request(DBOS.getHTTPHandlersCallback()!).get('/nosuchendpoint');
    expect(middlewareCounterG).toBe(3);
  });

  test('get-hello-name', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/hello/alice');
    expect(response.statusCode).toBe(200);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(response.body.message).toBe('hello, alice!');
    expect(middlewareCounter).toBe(1);
    expect(middlewareCounterG).toBe(1);
  });

  test('not-authenticated', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/requireduser?name=alice');
    expect(response.statusCode).toBe(401);
  });

  test('not-you', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/requireduser?name=alice&userid=go_away');
    expect(response.statusCode).toBe(401);
  });

  test('not-authorized', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/requireduser?name=alice&userid=bob');
    expect(response.statusCode).toBe(403);
  });

  test('authorized', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/requireduser?name=alice&userid=a_real_user');
    expect(response.statusCode).toBe(200);
  });

  // The handler is authorized, then its child workflow and transaction should also be authroized.
  test('cascade-authorized', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/workflow?name=alice&userid=a_real_user');
    expect(response.statusCode).toBe(200);

    const txnResponse = await request(DBOS.getHTTPHandlersCallback()!).get(
      '/transaction?name=alice&userid=a_real_user',
    );
    expect(txnResponse.statusCode).toBe(200);
  });

  // We can directly test a transaction with passed in authorizedRoles.
  test('direct-transaction-test', async () => {
    await DBOS.withAuthedContext('user', ['user'], async () => {
      const res = await DBOS.invoke(TestEndpointDefSec).testTranscation('alice');
      expect(res).toBe('hello 1');
    });

    // Unauthorized.
    await expect(DBOS.invoke(TestEndpointDefSec).testTranscation('alice')).rejects.toThrow(
      new DBOSNotAuthorizedError('User does not have a role with permission to call testTranscation', 403),
    );
  });

  async function authTestMiddleware(ctx: MiddlewareContext) {
    if (ctx.requiredRole.length > 0) {
      const { userid } = ctx.koaContext.request.query;
      const uid = userid?.toString();

      if (!uid || uid.length === 0) {
        return Promise.reject(new DBOSNotAuthorizedError('Not logged in.', 401));
      } else {
        if (uid === 'go_away') {
          return Promise.reject(new DBOSNotAuthorizedError('Go away.', 401));
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
  const testMiddleware: Middleware = async (ctx, next) => {
    middlewareCounter++;
    await next();
  };

  let middlewareCounter2 = 0;
  const testMiddleware2: Middleware = async (ctx, next) => {
    middlewareCounter2 = middlewareCounter2 + 1;
    await next();
  };

  let middlewareCounterG = 0;
  const testMiddlewareG: Middleware = async (ctx, next) => {
    middlewareCounterG = middlewareCounterG + 1;
    expect(DBOS.globalLogger).toBeDefined();
    await next();
  };

  @DefaultRequiredRole(['user'])
  @Authentication(authTestMiddleware)
  @KoaMiddleware(testMiddleware, testMiddleware2)
  @KoaGlobalMiddleware(testMiddlewareG)
  class TestEndpointDefSec {
    @RequiredRole([])
    @GetApi('/hello')
    static async hello(_ctx: HandlerContext) {
      return Promise.resolve({ message: 'hello!' });
    }

    @RequiredRole([])
    @GetApi('/hello/:name')
    static async helloName(_ctx: HandlerContext, name: string) {
      return Promise.resolve({ message: `hello, ${name}!` });
    }

    @GetApi('/requireduser')
    static async testAuth(_ctxt: HandlerContext, name: string) {
      return Promise.resolve(`Please say hello to ${name}`);
    }

    @Transaction()
    static async testTranscation(txnCtxt: TransactionContext<PoolClient>, name: string) {
      const { rows } = await txnCtxt.client.query<TestKvTable>(
        `INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`,
        [name],
      );
      return `hello ${rows[0].id}`;
    }

    @Workflow()
    static async testWorkflow(wfCtxt: WorkflowContext, name: string) {
      const res = await wfCtxt.invoke(TestEndpointDefSec).testTranscation(name);
      return res;
    }

    @GetApi('/workflow')
    static async testWfEndpoint(ctxt: HandlerContext, name: string) {
      return ctxt.invokeWorkflow(TestEndpointDefSec).testWorkflow(name);
    }

    @GetApi('/transaction')
    static async testTxnEndpoint(ctxt: HandlerContext, name: string) {
      return ctxt.invoke(TestEndpointDefSec).testTranscation(name);
    }
  }

  class SecondClass {
    @RequiredRole([])
    @GetApi('/goodbye')
    static async bye(_ctx: HandlerContext) {
      return Promise.resolve({ message: 'bye!' });
    }
  }
});
