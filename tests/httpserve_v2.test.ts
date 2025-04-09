/* eslint-disable @typescript-eslint/no-unsafe-member-access */

import bodyParser from '@koa/bodyparser';
import {
  ArgSource,
  ArgSources,
  Authentication,
  DBOS,
  DBOSResponseError,
  DefaultArgRequired,
  KoaBodyParser,
  MiddlewareContext,
  StatusString,
} from '../src';
import { DBOSNotAuthorizedError } from '../src/error';
import { WorkflowUUIDHeader } from '../src/httpServer/server';
import { generateDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from './helpers';
import request from 'supertest';
import { v1 as uuidv1, validate as uuidValidate } from 'uuid';
import { RequestIDHeader } from '../src/httpServer/middleware';
import { IncomingMessage } from 'http';

const testTableName = 'dbos_test_kv';

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

@DefaultArgRequired
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
class TestEndpoints {
  @DBOS.transaction()
  static async getKey() {
    return Promise.resolve();
  }

  @DBOS.getApi('/hello')
  static async hello() {
    return Promise.resolve({ message: 'hello!' });
  }

  @DBOS.getApi('/hello/:id')
  static async helloUrl(id: string) {
    // Customize status code and response.
    DBOS.koaContext.body = `wow ${id}`;
    DBOS.koaContext.status = 301;
    return Promise.resolve(`hello ${id}`);
  }

  @DBOS.getApi('/redirect')
  static async redirectUrl() {
    const url = DBOS.request.url || 'bad url'; // Get the raw url from request.
    DBOS.koaContext.redirect(url + '-dbos');
    return Promise.resolve();
  }

  @DBOS.getApi('/query')
  static async helloQuery(name: string) {
    DBOS.logger.info(`query with name ${name}`); // Test logging.
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.getApi('/querybody')
  static async helloQueryBody(@ArgSource(ArgSources.BODY) name: string) {
    DBOS.logger.info(`query with name ${name}`); // Test logging.
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.deleteApi('/testdeletequery')
  static async testdeletequeryparam(name: string) {
    DBOS.logger.info(`delete with param from query with name ${name}`);
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.deleteApi('/testdeleteurl/:name')
  static async testdeleteurlparam(name: string) {
    DBOS.logger.info(`delete with param from url with name ${name}`);
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.deleteApi('/testdeletebody')
  static async testdeletebodyparam(@ArgSource(ArgSources.BODY) name: string) {
    DBOS.logger.info(`delete with param from url with name ${name}`);
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.postApi('/testpost')
  static async testpost(name: string) {
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.putApi('/testput')
  static async testput(name: string) {
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.patchApi('/testpatch')
  static async testpatch(name: string) {
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.getApi('/dbos-error')
  @DBOS.transaction()
  static async dbosErr() {
    return Promise.reject(new DBOSResponseError('customize error', 503));
  }

  @DBOS.getApi('/handler/:name')
  static async testHandler(name: string) {
    const workflowUUID = DBOS.koaContext.get(WorkflowUUIDHeader);
    // Invoke a workflow using the given UUID.
    return await DBOS.withNextWorkflowID(workflowUUID, () => {
      return TestEndpoints.testWorkflow(name);
    });
  }

  @DBOS.getApi('/testStartWorkflow/:name')
  static async testStartWorkflow(name: string): Promise<string> {
    const wfh = await DBOS.startWorkflow(TestEndpoints).testWorkflow(name);
    return await wfh.getResult();
  }

  @DBOS.getApi('/testInvokeWorkflow/:name')
  static async testInvokeWorkflow(name: string): Promise<string> {
    return await TestEndpoints.testWorkflow(name);
  }

  @DBOS.postApi('/transaction/:name')
  @DBOS.transaction()
  static async testTransaction(name: string) {
    const { rows } = await DBOS.pgClient.query<TestKvTable>(
      `INSERT INTO ${testTableName}(id, value) VALUES (1, $1) RETURNING id`,
      [name],
    );
    return `hello ${rows[0].id}`;
  }

  @DBOS.getApi('/step/:input')
  @DBOS.step()
  static async testStep(input: string) {
    return Promise.resolve(input);
  }

  @DBOS.postApi('/workflow')
  @DBOS.workflow()
  static async testWorkflow(@ArgSource(ArgSources.QUERY) name: string) {
    const res = await TestEndpoints.testTransaction(name);
    return await TestEndpoints.testStep(res);
  }

  @DBOS.postApi('/error')
  @DBOS.workflow()
  static async testWorkflowError(name: string) {
    // This workflow should encounter duplicate primary key error.
    let res = await TestEndpoints.testTransaction(name);
    res = await TestEndpoints.testTransaction(name);
    return res;
  }

  @DBOS.getApi('/requireduser')
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

  @DBOS.getApi('/requireduser2')
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

async function main() {
  await TestEndpoints.getKey();
}

describe('dbos-v2api-tests-http', () => {
  beforeAll(async () => {
    const config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
    DBOS.setUpHandlerCallback();
  });

  beforeEach(async () => {
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INT PRIMARY KEY, value TEXT);`);
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  test('simple-functions', async () => {
    await main();
  }, 15000);

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
});
