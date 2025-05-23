import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client, Pool } from 'pg';
import { KnexDataSource } from './index';
import { dropDB, ensureDB } from './test-helpers';
import { randomUUID } from 'crypto';

const config = { client: 'pg', connection: { user: 'postgres', database: 'knex_ds_test_userdb' } };
const dataSource = new KnexDataSource('app-db', config);
DBOS.registerDataSource(dataSource);

describe('KnexDataSource', () => {
  const userDB = new Pool(config.connection);

  beforeAll(async () => {
    {
      const client = new Client({ ...config.connection, database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'knex_ds_test');
        await dropDB(client, 'knex_ds_test_dbos_sys');
        await dropDB(client, config.connection.database);
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

    await KnexDataSource.configure(config);
    DBOS.setConfig({ name: 'knex-ds-test' });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
    await userDB.end();
  });

  test('test dataSource.register function', async () => {
    const user = 'helloTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regHelloWorkflow1(user))).resolves.toEqual({
      user,
      greet_count: 1,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [workflowID]);
    expect(rows.length).toBe(1);
    expect(rows[0].workflow_id).toBe(workflowID);
    expect(rows[0].function_num).toBe(0);
    expect(JSON.parse(rows[0].output)).toEqual({ user, greet_count: 1 });
  });

  test('test dataSource.runAsTx function', async () => {
    const user = 'helloTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regHelloWorkflow2(user))).resolves.toEqual({
      user,
      greet_count: 1,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [workflowID]);
    expect(rows.length).toBe(1);
    expect(rows[0].workflow_id).toBe(workflowID);
    expect(rows[0].function_num).toBe(0);
    expect(JSON.parse(rows[0].output)).toEqual({ user, greet_count: 1 });
  });

  test('test readonly dataSource.register function', async () => {
    const user = 'readTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regReadWorkflow1(user))).resolves.toEqual({
      user,
      greet_count: 10,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [workflowID]);
    expect(rows.length).toBe(0);
  });

  test('test readonly dataSource.runAsTx function', async () => {
    const user = 'readTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regReadWorkflow2(user))).resolves.toEqual({
      user,
      greet_count: 10,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [workflowID]);
    expect(rows.length).toBe(0);
  });

  test('static test', async () => {
    const user = 'staticTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regStaticWorkflow(user))).resolves.toEqual([
      { user, greet_count: 1 },
      { user, greet_count: 1 },
    ]);
  });

  test('instance test', async () => {
    const user = 'instanceTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regInstanceWorkflow(user))).resolves.toEqual([
      { user, greet_count: 1 },
      { user, greet_count: 1 },
    ]);
  });
});

export interface greetings {
  name: string;
  greet_count: number;
}

async function helloFunction(user: string) {
  const rows = await KnexDataSource.client<greetings>('greetings')
    .insert({ name: user, greet_count: 1 })
    .onConflict('name')
    .merge({ greet_count: KnexDataSource.client.raw('greetings.greet_count + 1') })
    .returning('greet_count');
  const row = rows.length > 0 ? rows[0] : undefined;

  return { user, greet_count: row?.greet_count };
}

const regHelloFunction = dataSource.register(helloFunction, 'helloFunction');

async function readFunction(user: string) {
  const row = await KnexDataSource.client<greetings>('greetings').select('greet_count').where('name', user).first();
  return { user, greet_count: row?.greet_count };
}

const regReadFunction = dataSource.register(readFunction, 'readFunction', { readOnly: true });

async function helloWorkflow1(user: string) {
  return await regHelloFunction(user);
}

const regHelloWorkflow1 = DBOS.registerWorkflow(helloWorkflow1, { name: 'helloWorkflow1' });

async function helloWorkflow2(user: string) {
  return await dataSource.runTxStep(() => helloFunction(user), 'helloFunction');
}

const regHelloWorkflow2 = DBOS.registerWorkflow(helloWorkflow2, { name: 'helloWorkflow2' });

async function readWorkflow1(user: string) {
  return await regReadFunction(user);
}

const regReadWorkflow1 = DBOS.registerWorkflow(readWorkflow1, { name: 'readWorkflow1' });

async function readWorkflow2(user: string) {
  return await dataSource.runTxStep(() => readFunction(user), 'readFunction', { readOnly: true });
}

const regReadWorkflow2 = DBOS.registerWorkflow(readWorkflow2, { name: 'readWorkflow2' });

class StaticClass {
  static async helloFunction(user: string) {
    const rows = await KnexDataSource.client<greetings>('greetings')
      .insert({ name: user, greet_count: 1 })
      .onConflict('name')
      .merge({ greet_count: KnexDataSource.client.raw('greetings.greet_count + 1') })
      .returning('greet_count');
    const row = rows.length > 0 ? rows[0] : undefined;

    return { user, greet_count: row?.greet_count };
  }

  static async readFunction(user: string) {
    const row = await KnexDataSource.client<greetings>('greetings').select('greet_count').where('name', user).first();
    return { user, greet_count: row?.greet_count };
  }
}

StaticClass.helloFunction = dataSource.register(StaticClass.helloFunction, 'helloFunction');
StaticClass.readFunction = dataSource.register(StaticClass.readFunction, 'readFunction');

async function staticWorkflow(user: string) {
  const result = await StaticClass.helloFunction(user);
  const readResult = await StaticClass.readFunction(user);
  return [result, readResult];
}

const regStaticWorkflow = DBOS.registerWorkflow(staticWorkflow, { name: 'staticWorkflow' });

class InstanceClass {
  async helloFunction(user: string) {
    const rows = await KnexDataSource.client<greetings>('greetings')
      .insert({ name: user, greet_count: 1 })
      .onConflict('name')
      .merge({ greet_count: KnexDataSource.client.raw('greetings.greet_count + 1') })
      .returning('greet_count');
    const row = rows.length > 0 ? rows[0] : undefined;

    return { user, greet_count: row?.greet_count };
  }

  async readFunction(user: string) {
    const row = await KnexDataSource.client<greetings>('greetings').select('greet_count').where('name', user).first();
    return { user, greet_count: row?.greet_count };
  }
}

InstanceClass.prototype.helloFunction = dataSource.register(InstanceClass.prototype.helloFunction, 'helloFunction');
InstanceClass.prototype.readFunction = dataSource.register(InstanceClass.prototype.readFunction, 'readFunction');

async function instanceWorkflow(user: string) {
  const instance = new InstanceClass();
  const result = await instance.helloFunction(user);
  const readResult = await instance.readFunction(user);
  return [result, readResult];
}

const regInstanceWorkflow = DBOS.registerWorkflow(instanceWorkflow, { name: 'instanceWorkflow' });
