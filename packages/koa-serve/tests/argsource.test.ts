import Koa from 'koa';
import Router from '@koa/router';

import { DBOS } from '@dbos-inc/dbos-sdk';

import { ArgSources, DBOSKoa } from '../src';

import request from 'supertest';
import bodyParser from '@koa/bodyparser';

const dhttp = new DBOSKoa();

describe('httpserver-argsource-tests', () => {
  let app: Koa;
  let appRouter: Router;

  beforeAll(async () => {
    DBOS.setConfig({
      name: 'dbos-koa-test',
    });
    return Promise.resolve();
  });

  beforeEach(async () => {
    const _classes = [ArgTestEndpoints];
    await DBOS.launch();
    app = new Koa();
    appRouter = new Router();
    dhttp.registerWithApp(app, appRouter);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('get-query', async () => {
    const response1 = await request(app.callback()).get('/getquery?name=alice');
    expect(response1.statusCode).toBe(200);
    expect(response1.text).toBe('hello alice');
  });

  test('get-body', async () => {
    const response2 = await request(app.callback()).get('/getbody').send({ name: 'alice' });
    expect(response2.statusCode).toBe(200);
    expect(response2.text).toBe('hello alice');
  });

  test('get-default', async () => {
    const response1 = await request(app.callback()).get('/getdefault?name=alice');
    expect(response1.statusCode).toBe(200);
    expect(response1.text).toBe('hello alice');
  });

  test('get-auto', async () => {
    const response1 = await request(app.callback()).get('/getauto?name=alice');
    expect(response1.statusCode).toBe(200);
    expect(response1.text).toBe('hello alice');
    const response2 = await request(app.callback()).get('/getauto').send({ name: 'alice' });
    expect(response2.statusCode).toBe(200);
    expect(response2.text).toBe('hello alice');
  });

  test('post-query', async () => {
    const response1 = await request(app.callback()).post('/postquery?name=alice');
    expect(response1.statusCode).toBe(200);
    expect(response1.text).toBe('hello alice');
  });

  test('post-body', async () => {
    const response2 = await request(app.callback()).post('/postbody').send({ name: 'alice' });
    expect(response2.statusCode).toBe(200);
    expect(response2.text).toBe('hello alice');
  });

  test('post-default', async () => {
    const response2 = await request(app.callback()).post('/postdefault').send({ name: 'alice' });
    expect(response2.statusCode).toBe(200);
    expect(response2.text).toBe('hello alice');
  });

  test('post-auto', async () => {
    const response1 = await request(app.callback()).post('/postauto?name=alice');
    expect(response1.statusCode).toBe(200);
    expect(response1.text).toBe('hello alice');
    const response2 = await request(app.callback()).post('/postauto').send({ name: 'alice' });
    expect(response2.statusCode).toBe(200);
    expect(response2.text).toBe('hello alice');
  });

  @dhttp.koaBodyParser(
    bodyParser({
      enableTypes: ['json'],
      parsedMethods: ['GET', 'POST'],
    }),
  )
  class ArgTestEndpoints {
    @dhttp.getApi('/getquery')
    static async getQuery(@DBOSKoa.argSource(ArgSources.QUERY) name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.getApi('/getbody')
    static async getBody(@DBOSKoa.argSource(ArgSources.BODY) name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.getApi('/getdefault')
    static async getDefault(@DBOSKoa.argSource(ArgSources.DEFAULT) name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.getApi('/getauto')
    static async getAuto(@DBOSKoa.argSource(ArgSources.AUTO) name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.postApi('/postquery')
    static async postQuery(@DBOSKoa.argSource(ArgSources.QUERY) name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.postApi('/postbody')
    static async postBody(@DBOSKoa.argSource(ArgSources.BODY) name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.postApi('/postdefault')
    static async postDefault(@DBOSKoa.argSource(ArgSources.DEFAULT) name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @dhttp.postApi('/postauto')
    static async postAuto(@DBOSKoa.argSource(ArgSources.AUTO) name: string) {
      return Promise.resolve(`hello ${name}`);
    }
  }
});
