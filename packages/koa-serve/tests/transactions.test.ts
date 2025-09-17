import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client, Pool } from 'pg';
import { KnexDataSource } from '@dbos-inc/knex-datasource';
import { dropDB, ensureDB } from './test-helpers';
import Koa from 'koa';
import Router from '@koa/router';
import { DBOSKoa } from '../src';

import request from 'supertest';

const config = { client: 'pg', connection: { user: 'postgres', database: 'koa_knex_ds_test_userdb' } };
const knexds = new KnexDataSource('app-db', config);

const dhttp = new DBOSKoa();

interface greetings {
  name: string;
  greet_count: number;
}

describe('KnexDataSource', () => {
  const userDB = new Pool(config.connection);
  let app: Koa;
  let appRouter: Router;

  beforeAll(async () => {
    {
      const client = new Client({ ...config.connection, database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'koa_knex_ds_test', true);
        await dropDB(client, 'koa_knex_ds_test_dbos_sys', true);
        await dropDB(client, config.connection.database, true);
        await ensureDB(client, config.connection.database);
      } finally {
        await client.end();
      }
    }

    {
      const client = await userDB.connect();
      try {
        await client.query(
          'CREATE TABLE greetings(name text NOT NULL, greet_count integer DEFAULT 0, PRIMARY KEY(name))',
        );
      } finally {
        client.release();
      }
    }

    await KnexDataSource.initializeDBOSSchema(config);
  });

  afterAll(async () => {
    await userDB.end();
  });

  beforeEach(async () => {
    DBOS.setConfig({ name: 'koa-knex-ds-test' });
    await DBOS.launch();
    app = new Koa();
    appRouter = new Router();
    dhttp.registerWithApp(app, appRouter);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('use-dstx-and-koa', async () => {
    const u1 = await KnexKoa.insertOneWay('joe');
    expect(u1.user).toBe('joe');
    const u2 = await KnexKoa.insertTheOtherWay('jack');
    expect(u2.user).toBe('jack');

    const response1 = await request(app.callback()).get('/api/i1?user=john');
    expect(response1.statusCode).toBe(200);
    const response2 = await request(app.callback()).get('/api/i2?user=jeremy');
    expect(response2.statusCode).toBe(200);
  });
});

class KnexKoa {
  @knexds.transaction()
  @dhttp.getApi('/api/i2')
  static async insertTheOtherWay(user: string) {
    const rows = await knexds
      .client<greetings>('greetings')
      .insert({ name: user, greet_count: 1 })
      .onConflict('name')
      .merge({ greet_count: knexds.client.raw('greetings.greet_count + 1') })
      .returning('greet_count');

    const row = rows.length > 0 ? rows[0] : undefined;

    return { user, greet_count: row?.greet_count, now: Date.now() };
  }

  @dhttp.getApi('/api/i1')
  @knexds.transaction()
  static async insertOneWay(user: string) {
    const rows = await knexds
      .client<greetings>('greetings')
      .insert({ name: user, greet_count: 1 })
      .onConflict('name')
      .merge({ greet_count: knexds.client.raw('greetings.greet_count + 1') })
      .returning('greet_count');
    const row = rows.length > 0 ? rows[0] : undefined;

    return { user, greet_count: row?.greet_count, now: Date.now() };
  }
}
