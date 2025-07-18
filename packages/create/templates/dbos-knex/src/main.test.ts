import { DBOS } from '@dbos-inc/dbos-sdk';
import { app, Hello } from './main';
import request from 'supertest';

describe('operations-test', () => {
  beforeAll(async () => {
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  /**
   * Test the transaction.
   */
  test('test-transaction', async () => {
    await Hello.deleteUser('dbos');

    const res = await Hello.helloTransaction('dbos');
    expect(res).toMatch('Hello, dbos! You have been greeted');

    // Check the greet count.
    const rows = await Hello.getCount('dbos');
    expect(rows[0].greet_count).toBe(1);
  });

  /**
   * Test the HTTP endpoint.
   */
  test('test-endpoint', async () => {
    const traceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
    const res = await request(app).get('/greeting/dbos').set('traceparent', traceparent);
    expect(res.statusCode).toBe(200);
    expect(res.text).toMatch('Hello, dbos! You have been greeted');
  });
});
