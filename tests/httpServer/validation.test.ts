/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import { DBOS, ArgVarchar, ArgDate, DefaultArgRequired, DefaultArgOptional, ArgRequired, ArgOptional } from '../../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';
import request from 'supertest';
import { DBOSConfig } from '../../src/dbos-executor';

describe('httpserver-datavalidation-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    const _classes = [
      TestEndpointDataVal,
      DefaultArgToDefault,
      DefaultArgToOptional,
      DefaultArgToRequired,
      ArgNotMentioned,
    ];
    DBOS.setConfig(config);
    await DBOS.launch();
    DBOS.setUpHandlerCallback();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  test('get-hello', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/hello');
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('hello!');
  });

  test('not-there', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/nourl');
    expect(response.statusCode).toBe(404);
  });

  // Plain string
  test('no string (get)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/string');
    expect(response.statusCode).toBe(400);
  });
  test('no string (post)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/string');
    expect(response.statusCode).toBe(400);
  });
  test('no string (post) 2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/string').send({});
    expect(response.statusCode).toBe(400);
  });
  test('no string (post) - something else', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/string').send({ foo: 'bar' });
    expect(response.statusCode).toBe(400);
  });
  test('string get', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/string').query({ v: 'AAA' });
    expect(response.statusCode).toBe(200);
  });
  test('string post', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/string').send({ v: 'AAA' });
    expect(response.statusCode).toBe(200);
  });
  test('string post not a number', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/string').send({ v: 1234 });
    expect(response.statusCode).toBe(400);
  });

  // No string to optional arg -- in a workflow
  test('no string to workflow w optional arg', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/doworkflow').send({});
    expect(response.statusCode).toBe(200);
  });

  // Varchar(10)
  test('no string (get)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/varchar');
    expect(response.statusCode).toBe(400);
  });
  test('no string (post)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/varchar');
    expect(response.statusCode).toBe(400);
  });
  test('no string (post) 2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/varchar').send({});
    expect(response.statusCode).toBe(400);
  });
  test('string get', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/varchar').query({ v: 'AAA' });
    expect(response.statusCode).toBe(200);
  });
  test('string get - too long', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/varchar').query({ v: 'AAAaaaAAAaaa' });
    expect(response.statusCode).toBe(400);
  });
  test('string post', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/varchar').send({ v: 'AAA' });
    expect(response.statusCode).toBe(200);
  });
  test('string post - too long', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/varchar').send({ v: 'AAAaaaAAAaaa' });
    expect(response.statusCode).toBe(400);
  });
  test('string post not a number', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/varchar').send({ v: 1234 });
    expect(response.statusCode).toBe(400);
  });
  test('varchar post boolean', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/number').send({ v: false });
    expect(response.statusCode).toBe(400);
  });

  // Number (float)
  test('no number (get)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/number');
    expect(response.statusCode).toBe(400);
  });
  test('no number (post)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/number');
    expect(response.statusCode).toBe(400);
  });
  test('no number (post) 2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/number').send({});
    expect(response.statusCode).toBe(400);
  });
  test('number get', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/number').query({ v: '10.1' });
    expect(response.statusCode).toBe(200);
  });
  test('number get', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/number').query({ v: 10.5 });
    expect(response.statusCode).toBe(200);
  });
  test('number get - bogus value', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/number').query({ v: 'abc' });
    expect(response.statusCode).toBe(400);
  });
  test('number get - bigint', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/number').query({ v: 12345678901234567890n });
    expect(response.statusCode).toBe(200);
  });
  test('number post', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/number').send({ v: '20' });
    expect(response.statusCode).toBe(200);
  });
  test('number post', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/number').send({ v: 20.2 });
    expect(response.statusCode).toBe(200);
  });
  /* This fails for unknown reasons
  test("number post", async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post("/number")
    .send({v:0});
    expect(response.statusCode).toBe(200);
  });
  */
  test('number post', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/number').send({ v: -1 });
    expect(response.statusCode).toBe(200);
  });
  test('number post - bogus value', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/number').send({ v: 'AAAaaaAAAaaa' });
    expect(response.statusCode).toBe(400);
  });
  test('number post not a number', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/number').send({ v: false });
    expect(response.statusCode).toBe(400);
  });
  /* You can't do this - no bigint serialize to json
  test("number post bigint", async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post("/number")
    .send({v:234567890123456789n});
    expect(response.statusCode).toBe(200);
  });
  */
  test('number post bigint', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/number').send({ v: '12345678901234567890' });
    expect(response.statusCode).toBe(200);
  });

  // Boolean
  test('no boolean (get)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/boolean');
    expect(response.statusCode).toBe(400);
  });
  test('no boolean (post)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/boolean');
    expect(response.statusCode).toBe(400);
  });
  test('no boolean (post) 2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/boolean').send({});
    expect(response.statusCode).toBe(400);
  });

  test('true boolean (get)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/boolean').query({ v: 'true' });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: true');
  });
  test('true boolean (get) 2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/boolean').query({ v: true });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: true');
  });
  test('true boolean (get) 3', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/boolean').query({ v: 1 });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: true');
  });
  test('false boolean (get)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/boolean').query({ v: 'F' });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: false');
  });
  test('false boolean (get) 2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/boolean').query({ v: false });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: false');
  });
  test('false boolean (get) 3', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/boolean').query({ v: 0 });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: false');
  });

  test('true boolean (post)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/boolean').send({ v: 'true' });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: true');
  });
  test('true boolean (post) 2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/boolean').send({ v: true });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: true');
  });
  test('true boolean (post) 3', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/boolean').send({ v: 1 });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: true');
  });
  test('false boolean (post)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/boolean').send({ v: 'F' });
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe('This is a really nice boolean: false');
  });
  test('bad boolean 1', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/boolean').send({ v: 'A' });
    expect(response.statusCode).toBe(400);
  });
  test('bad boolean 2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/boolean').send({ v: 'falsy' });
    expect(response.statusCode).toBe(400);
  });
  test('bad boolean 1', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/boolean').send({ v: 2 });
    expect(response.statusCode).toBe(400);
  });

  // Date
  test('no date (get)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/date');
    expect(response.statusCode).toBe(400);
  });
  test('no date (post)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/date');
    expect(response.statusCode).toBe(400);
  });
  test('no date (post) 2', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/date').send({});
    expect(response.statusCode).toBe(400);
  });
  test('good date (get)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/date').query({ v: '2023-10-31' });
    expect(response.statusCode).toBe(200);
  });
  test('good date (post)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/date').send({ v: '2023-10-31' });
    expect(response.statusCode).toBe(200);
  });
  test('bad date (get)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).get('/date').query({ v: 'AAA' });
    expect(response.statusCode).toBe(400);
  });
  test('bad date (post)', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/date').send({ v: 'turnip' });
    expect(response.statusCode).toBe(400);
  });

  test('defined or not', async () => {
    const attempts = [
      ['/rrequired', undefined, 400],
      ['/rrequired', 'hasaval', 200],
      ['/rdefault', undefined, 400],
      ['/rdefault', 'hasaval', 200],
      ['/roptional', undefined, 200],
      ['/roptional', 'hasaval', 200],

      ['/orequired', undefined, 400],
      ['/orequired', 'hasaval', 200],
      ['/odefault', undefined, 200],
      ['/odefault', 'hasaval', 200],
      ['/ooptional', undefined, 200],
      ['/ooptional', 'hasaval', 200],

      ['/drequired', undefined, 400],
      ['/drequired', 'hasaval', 200],
      ['/ddefault', undefined, 200],
      ['/ddefault', 'hasaval', 200],
      ['/doptional', undefined, 200],
      ['/doptional', 'hasaval', 200],

      ['/nrequired', undefined, 200],
      ['/nrequired', 'hasaval', 200],
      ['/ndefault', undefined, 200],
      ['/ndefault', 'hasaval', 200],
      ['/noptional', undefined, 200],
      ['/noptional', 'hasaval', 200],
    ];

    for (const v of attempts) {
      const response = await request(DBOS.getHTTPHandlersCallback()!)
        .post(v[0] as string)
        .send({ v: v[1] });
      if (response.statusCode !== v[2]) {
        console.warn(`${v[0]} ${v[1]} ${v[2]} - ${response.statusCode}`);
      }
      expect(response.statusCode).toBe(v[2]);
    }
  });

  @DefaultArgRequired
  class TestEndpointDataVal {
    @DBOS.getApi('/hello')
    static async hello() {
      return Promise.resolve({ message: 'hello!' });
    }

    @DBOS.getApi('/string')
    static async checkStringG(v: string) {
      if (typeof v !== 'string') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice string: ${v}` });
    }

    @DBOS.postApi('/string')
    static async checkStringP(v: string) {
      if (typeof v !== 'string') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice string: ${v}` });
    }

    @DBOS.getApi('/varchar')
    static async checkVarcharG(@ArgVarchar(10) v: string) {
      if (typeof v !== 'string') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice string (limited length): ${v}` });
    }

    @DBOS.postApi('/varchar')
    static async checkVarcharP(@ArgVarchar(10) v: string) {
      if (typeof v !== 'string') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice string (limited length): ${v}` });
    }

    @DBOS.getApi('/number')
    static async checkNumberG(v: number) {
      if (typeof v !== 'number') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice number: ${v}` });
    }

    @DBOS.postApi('/number')
    static async checkNumberP(v: number) {
      if (typeof v !== 'number') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice number: ${v}` });
    }

    @DBOS.getApi('/bigint')
    static async checkBigintG(v: bigint) {
      if (typeof v !== 'bigint') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice bigint: ${v}` });
    }

    @DBOS.postApi('/bigint')
    static async checkBigintP(v: bigint) {
      if (typeof v !== 'bigint') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice bigint: ${v}` });
    }

    @DBOS.getApi('/date')
    static async checkDateG(@ArgDate() v: Date) {
      if (!(v instanceof Date)) {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice date: ${v.toISOString()}` });
    }

    @DBOS.postApi('/date')
    static async checkDateP(@ArgDate() v: Date) {
      if (!(v instanceof Date)) {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice date: ${v.toISOString()}` });
    }

    // This is in honor of Harry
    @DBOS.getApi('/boolean')
    static async checkBooleanG(v: boolean) {
      if (typeof v !== 'boolean') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice boolean: ${v}` });
    }

    @DBOS.postApi('/boolean')
    static async checkBooleanP(v: boolean) {
      if (typeof v !== 'boolean') {
        return Promise.reject(new Error('THIS SHOULD NEVER HAPPEN'));
      }
      return Promise.resolve({ message: `This is a really nice boolean: ${v}` });
    }

    // Types saved for another day - even the decorators are not there yet:
    //  Integer - not working
    //  Decimal
    //  UUID?
    //  JSON
  }

  @DefaultArgRequired
  class DefaultArgToRequired {
    @DBOS.postApi('/rrequired')
    static async checkReqValueR(@ArgRequired v: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }

    @DBOS.postApi('/roptional')
    static async checkOptValueR(@ArgOptional v?: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }

    @DBOS.postApi('/rdefault')
    static async checkDefValueR(v?: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }
  }

  @DefaultArgOptional
  class DefaultArgToOptional {
    @DBOS.postApi('/orequired')
    static async checkReqValueO(@ArgRequired v: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }

    @DBOS.postApi('/ooptional')
    static async checkOptValueO(@ArgOptional v?: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }

    @DBOS.postApi('/odefault')
    static async checkDefValueO(v?: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }
  }

  class DefaultArgToDefault {
    @DBOS.postApi('/drequired')
    static async checkReqValueD(@ArgRequired v: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }

    @DBOS.postApi('/doptional')
    static async checkOptValueD(@ArgOptional v?: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }

    @DBOS.postApi('/ddefault')
    static async checkDefValueD(v?: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }

    @DBOS.workflow()
    static async opworkflow(@ArgOptional v?: string) {
      return Promise.resolve({ message: v });
    }

    @DBOS.postApi('/doworkflow')
    static async doWorkflow(@ArgOptional v?: string) {
      return await DefaultArgToDefault.opworkflow(v);
    }
  }

  class ArgNotMentioned {
    @DBOS.postApi('/nrequired')
    static async checkReqValueO(v: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }

    @DBOS.postApi('/noptional')
    static async checkOptValueO(v?: string) {
      return Promise.resolve({ message: `Got string ${v}` });
    }

    @DBOS.postApi('/ndefault')
    static async checkDefValueO(v: string = 'b') {
      return Promise.resolve({ message: `Got string ${v}` });
    }
  }
});
