import { DBOS } from '@dbos-inc/dbos-sdk';
import { Hello } from './operations';
import request from 'supertest';

describe('operations-test', () => {
  beforeAll(async () => {
    await DBOS.launch();
    await DBOS.launchAppHTTPServer();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  /**
   * Test the HTTP endpoint.
   */
  test('test-greet', async () => {
    const res = await request(DBOS.getHTTPHandlersCallback()!).get('/greeting/dbos');
    expect(res.statusCode).toBe(200);
    expect(res.text).toMatch('Greeting 1: Hello, dbos!');
    expect(await Hello.helloTransaction('bob')).toMatch('Greeting 2: Hello, bob!');
  });
});
