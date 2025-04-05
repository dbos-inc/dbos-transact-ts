/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import {
  GetApi,
  Transaction,
  Workflow,
  MiddlewareContext,
  PostApi,
  RequiredRole,
  TransactionContext,
  WorkflowContext,
  StatusString,
  Step,
  StepContext,
  DBOS,
  DefaultArgRequired,
} from '../../src';
import { DeleteApi, PatchApi, PutApi } from '../../src';
import { WorkflowUUIDHeader } from '../../src/httpServer/server';
import { TestKvTable, generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';
import request from 'supertest';
import { ArgSource, HandlerContext } from '../../src/httpServer/handler';
import { ArgSources } from '../../src/httpServer/handlerTypes';
import { Authentication, KoaBodyParser, RequestIDHeader } from '../../src/httpServer/middleware';
import { v1 as uuidv1, validate as uuidValidate } from 'uuid';
import { DBOSConfig, DBOSExecutor } from '../../src/dbos-executor';
import { DBOSNotAuthorizedError, DBOSResponseError } from '../../src/error';
import { PoolClient } from 'pg';
import { IncomingMessage } from 'http';
import { bodyParser } from '@koa/bodyparser';

describe('httpserver-tests', () => {
  const testTableName = 'dbos_test_kv';

  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    const _classes = [TestEndpoints];
    await DBOS.launch();
    DBOS.setUpHandlerCallback();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INT PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('get-hello', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/hello');
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('hello!');
    const requestID: string = response.headers[RequestIDHeader.toLowerCase()];
    // Expect uuidValidate to be true
    expect(uuidValidate(requestID)).toBe(true);
  });

  test('get-url', async () => {
    const requestID = 'my-request-id';
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/hello/alice').set(RequestIDHeader, requestID);
    expect(response.statusCode).toBe(301);
    expect(response.text).toBe('wow alice');
    expect(response.headers[RequestIDHeader.toLowerCase()]).toBe(requestID);
  });

  test('get-query', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/query?name=alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('get-querybody', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/querybody').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('delete-query', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).delete('/testdeletequery?name=alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('delete-url', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).delete('/testdeleteurl/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('delete-body', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).delete('/testdeletebody').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('post-test', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/testpost').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('post-test-custom-body', async () => {
    let response = await request(DBOS.getHTTPHandlersCallback()!)
      .post('/testpost')
      .set('Content-Type', 'application/custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
    response = await request(DBOS.getHTTPHandlersCallback()!)
      .post('/testpost')
      .set('Content-Type', 'application/rejected-custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(400);
  });

  test('put-test', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).put('/testput').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('put-test-custom-body', async () => {
    let response = await request(DBOS.getHTTPHandlersCallback()!)
      .put('/testput')
      .set('Content-Type', 'application/custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
    response = await request(DBOS.getHTTPHandlersCallback()!)
      .put('/testput')
      .set('Content-Type', 'application/rejected-custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(400);
  });

  test('patch-test', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).patch('/testpatch').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('patch-test-custom-body', async () => {
    let response = await request(DBOS.getHTTPHandlersCallback()!)
      .patch('/testpatch')
      .set('Content-Type', 'application/custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
    response = await request(DBOS.getHTTPHandlersCallback()!)
      .patch('/testpatch')
      .set('Content-Type', 'application/rejected-custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(400);
  });

  test('endpoint-transaction', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/transaction/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  test('endpoint-step', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/step/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');
  });

  test('endpoint-workflow', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow?name=alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  test('endpoint-error', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/error').send({ name: 'alice' });
    expect(response.statusCode).toBe(500);
    expect(response.body.details.code).toBe('23505'); // Should be the expected error.
  });

  test('endpoint-handler', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/handler/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  test('endpoint-testStartWorkflow', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/testStartWorkflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  test('endpoint-testInvokeWorkflow', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/testInvokeWorkflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  // This feels unclean, but supertest doesn't expose the error message the people we want. See:
  //   https://github.com/ladjs/supertest/issues/95
  interface Res {
    res: IncomingMessage;
  }

  test('response-error', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/dbos-error');
    expect(response.statusCode).toBe(503);
    expect((response as unknown as Res).res.statusMessage).toBe('customize error');
    expect(response.body.message).toBe('customize error');
  });

  test('datavalidation-error', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/query');
    expect(response.statusCode).toBe(400);
    expect(response.body.details.dbosErrorCode).toBe(9);
  });

  test('dbos-redirect', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/redirect');
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe('/redirect-dbos');
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

  test('not-authenticated2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/requireduser2?name=alice');
    expect(response.statusCode).toBe(401);
  });

  test('not-you2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/requireduser2?name=alice&userid=go_away');
    expect(response.statusCode).toBe(401);
  });

  test('not-authorized2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/requireduser2?name=alice&userid=bob');
    expect(response.statusCode).toBe(403);
  });

  test('authorized2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/requireduser2?name=alice&userid=a_real_user');
    expect(response.statusCode).toBe(200);
  });

  test('test-workflowUUID-header', async () => {
    const workflowUUID = uuidv1();
    const response = await request(DBOS.getHTTPHandlersCallback()!)
      .post('/workflow?name=bob')
      .set({ 'dbos-idempotency-key': workflowUUID });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');

    // Retrieve the workflow with UUID.
    const retrievedHandle = DBOS.retrieveWorkflow(workflowUUID);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getResult()).resolves.toBe('hello 1');
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
      request: { url: '/workflow?name=bob' },
    });
  });

  test('endpoint-handler-UUID', async () => {
    const workflowUUID = uuidv1();
    const response = await request(DBOS.getHTTPHandlersCallback()!)
      .get('/handler/bob')
      .set({ 'dbos-idempotency-key': workflowUUID });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');

    // Retrieve the workflow with UUID.
    const retrievedHandle = DBOS.retrieveWorkflow(workflowUUID);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getResult()).resolves.toBe('hello 1');
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
      request: { url: '/handler/bob' },
    });
  });

  async function testAuthMiddlware(ctx: MiddlewareContext) {
    if (ctx.requiredRole.length > 0) {
      const { userid } = ctx.koaContext.request.query;
      const uid = userid?.toString();

      if (!uid || uid.length === 0) {
        const err = new DBOSNotAuthorizedError('Not logged in.', 401);
        throw err;
      } else {
        if (uid === 'go_away') {
          throw new DBOSNotAuthorizedError('Go away.', 401);
        }
        return Promise.resolve({
          authenticatedUser: uid,
          authenticatedRoles: uid === 'a_real_user' ? ['user'] : ['other'],
        });
      }
    }
    return;
  }

  type TestTransactionContext = TransactionContext<PoolClient>;

  @Authentication(testAuthMiddlware)
  @KoaBodyParser(
    bodyParser({
      extendTypes: {
        json: ['application/json', 'application/custom-content-type'],
      },
      encoding: 'utf-8',
      parsedMethods: ['POST', 'PUT', 'PATCH', 'GET', 'DELETE'],
    }),
  )
  @DefaultArgRequired
  class TestEndpoints {
    @GetApi('/hello')
    static async hello(_ctx: HandlerContext) {
      return Promise.resolve({ message: 'hello!' });
    }

    @GetApi('/hello/:id')
    static async helloUrl(ctx: HandlerContext, id: string) {
      // Customize status code and response.
      ctx.koaContext.body = `wow ${id}`;
      ctx.koaContext.status = 301;
      return Promise.resolve(`hello ${id}`);
    }

    @GetApi('/redirect')
    static async redirectUrl(ctx: HandlerContext) {
      const url = ctx.request.url || 'bad url'; // Get the raw url from request.
      ctx.koaContext.redirect(url + '-dbos');
      return Promise.resolve();
    }

    @GetApi('/query')
    static async helloQuery(ctx: HandlerContext, name: string) {
      ctx.logger.info(`query with name ${name}`); // Test logging.
      return Promise.resolve(`hello ${name}`);
    }

    @GetApi('/querybody')
    static async helloQueryBody(ctx: HandlerContext, @ArgSource(ArgSources.BODY) name: string) {
      ctx.logger.info(`query with name ${name}`); // Test logging.
      return Promise.resolve(`hello ${name}`);
    }

    @DeleteApi('/testdeletequery')
    static async testdeletequeryparam(ctx: HandlerContext, name: string) {
      ctx.logger.info(`delete with param from query with name ${name}`);
      return Promise.resolve(`hello ${name}`);
    }

    @DeleteApi('/testdeleteurl/:name')
    static async testdeleteurlparam(ctx: HandlerContext, name: string) {
      ctx.logger.info(`delete with param from url with name ${name}`);
      return Promise.resolve(`hello ${name}`);
    }

    @DeleteApi('/testdeletebody')
    static async testdeletebodyparam(ctx: HandlerContext, @ArgSource(ArgSources.BODY) name: string) {
      ctx.logger.info(`delete with param from url with name ${name}`);
      return Promise.resolve(`hello ${name}`);
    }

    @PostApi('/testpost')
    static async testpost(_ctx: HandlerContext, name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @PutApi('/testput')
    static async testput(_ctx: HandlerContext, name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @PatchApi('/testpatch')
    static async testpatch(_ctx: HandlerContext, name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @GetApi('/dbos-error')
    @Transaction()
    static async dbosErr(_ctx: TestTransactionContext) {
      return Promise.reject(new DBOSResponseError('customize error', 503));
    }

    @GetApi('/handler/:name')
    static async testHandler(ctxt: HandlerContext, name: string) {
      const workflowUUID = ctxt.koaContext.get(WorkflowUUIDHeader);
      // Invoke a workflow using the given UUID.
      return ctxt
        .invoke(TestEndpoints, workflowUUID)
        .testWorkflow(name)
        .then((x) => x.getResult());
    }

    @GetApi('/testStartWorkflow/:name')
    static async testStartWorkflow(ctxt: HandlerContext, name: string): Promise<string> {
      return ctxt
        .startWorkflow(TestEndpoints)
        .testWorkflow(name)
        .then((x) => x.getResult());
    }

    @GetApi('/testInvokeWorkflow/:name')
    static async testInvokeWorkflow(ctxt: HandlerContext, name: string): Promise<string> {
      return ctxt.invokeWorkflow(TestEndpoints).testWorkflow(name);
    }

    @PostApi('/transaction/:name')
    @Transaction()
    static async testTransaction(txnCtxt: TestTransactionContext, name: string) {
      const { rows } = await txnCtxt.client.query<TestKvTable>(
        `INSERT INTO ${testTableName}(id, value) VALUES (1, $1) RETURNING id`,
        [name],
      );
      return `hello ${rows[0].id}`;
    }

    @GetApi('/step/:input')
    @Step()
    static async testStep(_ctxt: StepContext, input: string) {
      return Promise.resolve(input);
    }

    @PostApi('/workflow')
    @Workflow()
    static async testWorkflow(wfCtxt: WorkflowContext, @ArgSource(ArgSources.QUERY) name: string) {
      const res = await wfCtxt.invoke(TestEndpoints).testTransaction(name);
      return wfCtxt.invoke(TestEndpoints).testStep(res);
    }

    @PostApi('/error')
    @Workflow()
    static async testWorkflowError(wfCtxt: WorkflowContext, name: string) {
      // This workflow should encounter duplicate primary key error.
      let res = await wfCtxt.invoke(TestEndpoints).testTransaction(name);
      res = await wfCtxt.invoke(TestEndpoints).testTransaction(name);
      return res;
    }

    @GetApi('/requireduser')
    @RequiredRole(['user'])
    static async testAuth(ctxt: HandlerContext, name: string) {
      if (ctxt.authenticatedUser !== 'a_real_user') {
        throw new DBOSResponseError('uid not a real user!', 400);
      }
      if (!ctxt.authenticatedRoles.includes('user')) {
        throw new DBOSResponseError("roles don't include user!", 400);
      }
      if (ctxt.assumedRole !== 'user') {
        throw new DBOSResponseError('Should never happen! Not assumed to be user', 400);
      }
      return Promise.resolve(`Please say hello to ${name}`);
    }

    @GetApi('/requireduser2')
    @RequiredRole(['user'])
    static async testAuth2(_ctxt: HandlerContext, name: string) {
      if (DBOS.authenticatedUser !== 'a_real_user') {
        throw new DBOSResponseError('uid not a real user!', 400);
      }
      if (!DBOS.authenticatedRoles.includes('user')) {
        throw new DBOSResponseError("roles don't include user!", 400);
      }
      if (DBOS.assumedRole !== 'user') {
        throw new DBOSResponseError('Should never happen! Not assumed to be user', 400);
      }
      return Promise.resolve(`Please say hello to ${name}`);
    }
  }
});
