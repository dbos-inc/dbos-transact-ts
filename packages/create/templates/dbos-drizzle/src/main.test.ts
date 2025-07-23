import { DBOS } from '@dbos-inc/dbos-sdk';
import { app, config, Hello } from './main';
import request from 'supertest';
import { Client } from 'pg';

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
    const res = await Hello.helloTransaction('dbos');
    expect(res).toMatch('Hello, dbos! We have made');

    const client = new Client(config);
    try {
      await client.connect();
      const rows = await client.query('SELECT * FROM dbos_hello WHERE greet_count=1');
      expect(rows.rows.length).toEqual(1);
    } finally {
      await client.end();
    }
  });

  /**
   * Test the HTTP endpoint.
   */
  test('test-endpoint', async () => {
    const res = await request(app).get('/greeting/dbos');
    expect(res.statusCode).toBe(200);
    expect(res.text).toMatch('Hello, dbos! We have made');
  });
});
