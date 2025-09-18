/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import Koa from 'koa';
import Router from '@koa/router';

import { DBOS, DBOSResponseError, Error as DBOSErrors, StatusString } from '@dbos-inc/dbos-sdk';

import { DBOSKoa, DBOSKoaAuthContext, RequestIDHeader, WorkflowIDHeader } from '../src';

import request from 'supertest';

const dhttp = new DBOSKoa();

interface TestKvTable {
  id?: number;
  value?: string;
}

import { randomUUID } from 'node:crypto';
import { IncomingMessage } from 'http';
import { bodyParser } from '@koa/bodyparser';

// copied from https://github.com/uuidjs/uuid project
function uuidValidate(uuid: string) {
  const regex =
    /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$/i;
  return regex.test(uuid);
}
describe('httpserver-tests', () => {
  let app: Koa;
  let appRouter: Router;

  const testTableName = 'dbos_test_kv';

  beforeAll(async () => {
    DBOS.setConfig({
      name: 'dbos-koa-test',
    });
    return Promise.resolve();
  });

  beforeEach(async () => {
    const _classes = [TestEndpoints];
    await DBOS.launch();
    app = new Koa();
    appRouter = new Router();
    dhttp.registerWithApp(app, appRouter);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('get-hello', async () => {
    const response = await request(app.callback()).get('/hello');
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('hello!');
    const requestID: string = response.headers[RequestIDHeader.toLowerCase()];
    // Expect uuidValidate to be true
    expect(uuidValidate(requestID)).toBe(true);
  });

  test('get-url', async () => {
    const requestID = 'my-request-id';
    const response = await request(app.callback()).get('/hello/alice').set(RequestIDHeader, requestID);
    expect(response.statusCode).toBe(301);
    expect(response.text).toBe('wow alice');
    expect(response.headers[RequestIDHeader.toLowerCase()]).toBe(requestID);
  });

  test('get-query', async () => {
    const response = await request(app.callback()).get('/query?name=alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('get-querybody', async () => {
    const response = await request(app.callback()).get('/querybody').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('delete-query', async () => {
    const response = await request(app.callback()).delete('/testdeletequery?name=alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('delete-url', async () => {
    const response = await request(app.callback()).delete('/testdeleteurl/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('delete-body', async () => {
    const response = await request(app.callback()).delete('/testdeletebody').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('post-test', async () => {
    const response = await request(app.callback()).post('/testpost').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('post-test-custom-body', async () => {
    let response = await request(app.callback())
      .post('/testpost')
      .set('Content-Type', 'application/custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
    response = await request(app.callback())
      .post('/testpost')
      .set('Content-Type', 'application/rejected-custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(400);
  });

  test('put-test', async () => {
    const response = await request(app.callback()).put('/testput').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('put-test-custom-body', async () => {
    let response = await request(app.callback())
      .put('/testput')
      .set('Content-Type', 'application/custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
    response = await request(app.callback())
      .put('/testput')
      .set('Content-Type', 'application/rejected-custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(400);
  });

  test('patch-test', async () => {
    const response = await request(app.callback()).patch('/testpatch').send({ name: 'alice' });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
  });

  test('patch-test-custom-body', async () => {
    let response = await request(app.callback())
      .patch('/testpatch')
      .set('Content-Type', 'application/custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello alice');
    response = await request(app.callback())
      .patch('/testpatch')
      .set('Content-Type', 'application/rejected-custom-content-type')
      .send(JSON.stringify({ name: 'alice' }));
    expect(response.statusCode).toBe(400);
  });

  test('endpoint-transaction', async () => {
    const response = await request(app.callback()).post('/transaction/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  test('endpoint-step', async () => {
    const response = await request(app.callback()).get('/step/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');
  });

  test('endpoint-workflow', async () => {
    const response = await request(app.callback()).post('/workflow?name=alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  test('endpoint-error', async () => {
    const response = await request(app.callback()).post('/error').send({ name: 'alice' });
    expect(response.statusCode).toBe(500);
  });

  test('endpoint-handler', async () => {
    const response = await request(app.callback()).get('/handler/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  test('endpoint-testStartWorkflow', async () => {
    const response = await request(app.callback()).get('/testStartWorkflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  test('endpoint-testInvokeWorkflow', async () => {
    const response = await request(app.callback()).get('/testInvokeWorkflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');
  });

  // This feels unclean, but supertest doesn't expose the error message the people we want. See:
  //   https://github.com/ladjs/supertest/issues/95
  interface Res {
    res: IncomingMessage;
  }

  test('response-error', async () => {
    const response = await request(app.callback()).get('/dbos-error');
    expect(response.statusCode).toBe(503);
    expect((response as unknown as Res).res.statusMessage).toBe('customize error');
    expect(response.body.message).toBe('customize error');
  });

  test('datavalidation-error', async () => {
    const response = await request(app.callback()).get('/query');
    expect(response.statusCode).toBe(400);
    expect(response.body.details.dbosErrorCode).toBe(9);
  });

  test('dbos-redirect', async () => {
    const response = await request(app.callback()).get('/redirect');
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe('/redirect-dbos');
  });

  test('request-is-persisted', async () => {
    const workflowID = randomUUID();
    const response = await request(app.callback()).get('/check-url').set({ 'dbos-idempotency-key': workflowID });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('/check-url');

    // Retrieve the workflow with WFID.
    const retrievedHandle = DBOS.retrieveWorkflow(workflowID);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getResult()).resolves.toBe('/check-url');
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Start another WF based on that...
    const wfh = await DBOS.forkWorkflow(workflowID, 0);
    await expect(wfh.getResult()).resolves.toBe(`/check-url`);
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

  test('not-authenticated2', async () => {
    const response = await request(app.callback()).get('/requireduser2?name=alice');
    expect(response.statusCode).toBe(401);
  });

  test('not-you2', async () => {
    const response = await request(app.callback()).get('/requireduser2?name=alice&userid=go_away');
    expect(response.statusCode).toBe(401);
  });

  test('not-authorized2', async () => {
    const response = await request(app.callback()).get('/requireduser2?name=alice&userid=bob');
    expect(response.statusCode).toBe(403);
  });

  test('authorized2', async () => {
    const response = await request(app.callback()).get('/requireduser2?name=alice&userid=a_real_user');
    expect(response.statusCode).toBe(200);
  });

  test('test-workflowID-header', async () => {
    const workflowID = randomUUID();
    const response = await request(app.callback())
      .post('/workflow?name=bob')
      .set({ 'dbos-idempotency-key': workflowID });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');

    // Retrieve the workflow with WFID.
    const retrievedHandle = DBOS.retrieveWorkflow(workflowID);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getResult()).resolves.toBe('hello 1');
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  test('endpoint-handler-WFID', async () => {
    const workflowID = randomUUID();
    const response = await request(app.callback()).get('/handler/bob').set({ 'dbos-idempotency-key': workflowID });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('hello 1');

    // Retrieve the workflow with WFID.
    const retrievedHandle = DBOS.retrieveWorkflow(workflowID);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getResult()).resolves.toBe('hello 1');
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  async function testAuthMiddlware(ctx: DBOSKoaAuthContext) {
    if (ctx.requiredRole.length > 0) {
      const { userid } = ctx.koaContext.request.query;
      const uid = userid?.toString();

      if (!uid || uid.length === 0) {
        const err = new DBOSErrors.DBOSNotAuthorizedError('Not logged in.', 401);
        throw err;
      } else {
        if (uid === 'go_away') {
          throw new DBOSErrors.DBOSNotAuthorizedError('Go away.', 401);
        }
        return Promise.resolve({
          authenticatedUser: uid,
          authenticatedRoles: uid === 'a_real_user' ? ['user'] : ['other'],
        });
      }
    }
    return;
  }

  @dhttp.authentication(testAuthMiddlware)
  @dhttp.koaBodyParser(
    bodyParser({
      extendTypes: {
        json: ['application/json', 'application/custom-content-type'],
      },
      encoding: 'utf-8',
      parsedMethods: ['POST', 'PUT', 'PATCH', 'GET', 'DELETE'],
    }),
  )
  @DBOSKoa.defaultArgRequired
  class TestEndpoints {
    @dhttp.getApi('/hello')
    static async hello() {
      return Promise.resolve({ message: 'hello!' });
    }

    @dhttp.getApi('/hello/:id')
    static async helloUrl(id: string) {
      // Customize status code and response.
      DBOSKoa.koaContext.body = `wow ${id}`;
      DBOSKoa.koaContext.status = 301;
      return Promise.resolve(`hello ${id}`);
    }

    @dhttp.getApi('/redirect')
    static async redirectUrl() {
      const url = DBOSKoa.httpRequest.url || 'bad url'; // Get the raw url from request.
      DBOSKoa.koaContext.redirect(url + '-dbos');
      return Promise.resolve();
    }

    @dhttp.getApi('/check-url')
    @DBOS.workflow()
    static async returnURL() {
      const url = DBOSKoa.httpRequest.url || 'bad url'; // Get the raw url from request.
      return Promise.resolve(url);
    }

    @dhttp.getApi('/query')
    static async helloQuery(name: string) {
      DBOS.logger.info(`query with name ${name}`); // Test logging.
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.getApi('/querybody')
    static async helloQueryBody(name: string) {
      DBOS.logger.info(`query with name ${name}`); // Test logging.
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.deleteApi('/testdeletequery')
    static async testdeletequeryparam(name: string) {
      DBOS.logger.info(`delete with param from query with name ${name}`);
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.deleteApi('/testdeleteurl/:name')
    static async testdeleteurlparam(name: string) {
      DBOS.logger.info(`delete with param from url with name ${name}`);
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.deleteApi('/testdeletebody')
    static async testdeletebodyparam(name: string) {
      DBOS.logger.info(`delete with param from url with name ${name}`);
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.postApi('/testpost')
    static async testpost(name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.putApi('/testput')
    static async testput(name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.patchApi('/testpatch')
    static async testpatch(name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.getApi('/dbos-error')
    @DBOS.step()
    static async dbosErr() {
      return Promise.reject(new DBOSResponseError('customize error', 503));
    }

    @dhttp.getApi('/handler/:name')
    static async testHandler(name: string) {
      const workflowID: string = DBOSKoa.koaContext.get(WorkflowIDHeader);
      // Invoke a workflow using the given ID.
      return DBOS.startWorkflow(TestEndpoints, { workflowID })
        .testWorkflow(name)
        .then((x) => x.getResult());
    }

    @dhttp.getApi('/testStartWorkflow/:name')
    static async testStartWorkflow(name: string): Promise<string> {
      return DBOS.startWorkflow(TestEndpoints)
        .testWorkflow(name)
        .then((x) => x.getResult());
    }

    @dhttp.getApi('/testInvokeWorkflow/:name')
    static async testInvokeWorkflow(name: string): Promise<string> {
      return await TestEndpoints.testWorkflow(name);
    }

    @dhttp.postApi('/transaction/:name')
    @DBOS.step()
    static async testTransaction(name: string) {
      void name;
      return Promise.resolve(`hello 1`);
    }

    @dhttp.getApi('/step/:input')
    @DBOS.step()
    static async testStep(input: string) {
      return Promise.resolve(input);
    }

    @dhttp.postApi('/workflow')
    @DBOS.workflow()
    static async testWorkflow(name: string) {
      const res = await TestEndpoints.testTransaction(name);
      return TestEndpoints.testStep(res);
    }

    @dhttp.postApi('/error')
    @DBOS.workflow()
    static async testWorkflowError(name: string) {
      void name;
      // This workflow should encounter duplicate primary key error.
      throw Error('fail');
    }

    @dhttp.getApi('/requireduser')
    @DBOS.requiredRole(['user'])
    static async testAuth(name: string) {
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

    @dhttp.getApi('/requireduser2')
    @DBOS.requiredRole(['user'])
    static async testAuth2(name: string) {
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
