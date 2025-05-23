import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client, Pool } from 'pg';
import { KnexDataSource } from '../index';
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

  test('insert dataSource.register function', async () => {
    const user = 'helloTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regInsertWorfklowReg(user))).resolves.toEqual({
      user,
      greet_count: 1,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [workflowID]);
    expect(rows.length).toBe(1);
    expect(rows[0].workflow_id).toBe(workflowID);
    expect(rows[0].function_num).toBe(0);
    expect(JSON.parse(rows[0].output)).toEqual({ user, greet_count: 1 });
  });

  test('insert dataSource.runAsTx function', async () => {
    const user = 'helloTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regInsertWorfklowRunTx(user))).resolves.toEqual({
      user,
      greet_count: 1,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [workflowID]);
    expect(rows.length).toBe(1);
    expect(rows[0].workflow_id).toBe(workflowID);
    expect(rows[0].function_num).toBe(0);
    expect(JSON.parse(rows[0].output)).toEqual({ user, greet_count: 1 });
  });

  test('error dataSource.register function', async () => {
    const user = 'errorTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowReg(user))).rejects.toThrow('test error');

    const { rows: txOutput } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [
      workflowID,
    ]);
    expect(txOutput.length).toBe(0);

    const { rows } = await userDB.query('SELECT * FROM greetings WHERE name = $1', [user]);
    expect(rows.length).toBe(1);
    expect(rows[0].greet_count).toBe(10);
  });

  test('error dataSource.runAsTx function', async () => {
    const user = 'errorTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowRunTx(user))).rejects.toThrow('test error');

    const { rows: txOutput } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [
      workflowID,
    ]);
    expect(txOutput.length).toBe(0);

    const { rows } = await userDB.query('SELECT * FROM greetings WHERE name = $1', [user]);
    expect(rows.length).toBe(1);
    expect(rows[0].greet_count).toBe(10);
  });

  test('readonly dataSource.register function', async () => {
    const user = 'readTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regReadWorkflowReg(user))).resolves.toEqual({
      user,
      greet_count: 10,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [workflowID]);
    expect(rows.length).toBe(0);
  });

  test('readonly dataSource.runAsTx function', async () => {
    const user = 'readTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regReadWorkflowRunTx(user))).resolves.toEqual({
      user,
      greet_count: 10,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_outputs WHERE workflow_id = $1', [workflowID]);
    expect(rows.length).toBe(0);
  });

  test('static dataSource.register methods', async () => {
    const user = 'staticTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regStaticWorkflow(user))).resolves.toEqual([
      { user, greet_count: 1 },
      { user, greet_count: 1 },
    ]);
  });

  test('instance dataSource.register methods', async () => {
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

async function insertFunction(user: string) {
  const rows = await KnexDataSource.client<greetings>('greetings')
    .insert({ name: user, greet_count: 1 })
    .onConflict('name')
    .merge({ greet_count: KnexDataSource.client.raw('greetings.greet_count + 1') })
    .returning('greet_count');
  const row = rows.length > 0 ? rows[0] : undefined;

  return { user, greet_count: row?.greet_count };
}

async function errorFunction(user: string) {
  const result = await insertFunction(user);
  throw new Error('test error');
  return result;
}

async function readFunction(user: string) {
  const row = await KnexDataSource.client<greetings>('greetings').select('greet_count').where('name', user).first();
  return { user, greet_count: row?.greet_count };
}

const regInsertFunction = dataSource.register(insertFunction, 'insertFunction');
const regErrorFunction = dataSource.register(errorFunction, 'errorFunction');
const regReadFunction = dataSource.register(readFunction, 'readFunction', { readOnly: true });

class StaticClass {
  static async insertFunction(user: string) {
    return await insertFunction(user);
  }

  static async readFunction(user: string) {
    return await readFunction(user);
  }
}

StaticClass.insertFunction = dataSource.register(StaticClass.insertFunction, 'insertFunction');
StaticClass.readFunction = dataSource.register(StaticClass.readFunction, 'readFunction');

class InstanceClass {
  async insertFunction(user: string) {
    return await insertFunction(user);
  }

  async readFunction(user: string) {
    return await readFunction(user);
  }
}

InstanceClass.prototype.insertFunction = dataSource.register(InstanceClass.prototype.insertFunction, 'insertFunction');
InstanceClass.prototype.readFunction = dataSource.register(InstanceClass.prototype.readFunction, 'readFunction');

async function insertWorkflowReg(user: string) {
  return await regInsertFunction(user);
}

async function insertWorkflowRunTx(user: string) {
  return await dataSource.runTxStep(() => insertFunction(user), 'insertFunction');
}

async function errorWorkflowReg(user: string) {
  return await regErrorFunction(user);
}

async function errorWorkflowRunTx(user: string) {
  return await dataSource.runTxStep(() => errorFunction(user), 'errorFunction');
}

async function readWorkflowReg(user: string) {
  return await regReadFunction(user);
}

async function readWorkflowRunTx(user: string) {
  return await dataSource.runTxStep(() => readFunction(user), 'readFunction', { readOnly: true });
}

async function staticWorkflow(user: string) {
  const result = await StaticClass.insertFunction(user);
  const readResult = await StaticClass.readFunction(user);
  return [result, readResult];
}

async function instanceWorkflow(user: string) {
  const instance = new InstanceClass();
  const result = await instance.insertFunction(user);
  const readResult = await instance.readFunction(user);
  return [result, readResult];
}

const regInsertWorfklowReg = DBOS.registerWorkflow(insertWorkflowReg, { name: 'insertWorkflowReg' });
const regInsertWorfklowRunTx = DBOS.registerWorkflow(insertWorkflowRunTx, { name: 'insertWorkflowRunTx' });
const regErrorWorkflowReg = DBOS.registerWorkflow(errorWorkflowReg, { name: 'errorWorkflowReg' });
const regErrorWorkflowRunTx = DBOS.registerWorkflow(errorWorkflowRunTx, { name: 'errorWorkflowRunTx' });
const regReadWorkflowReg = DBOS.registerWorkflow(readWorkflowReg, { name: 'readWorkflowReg' });
const regReadWorkflowRunTx = DBOS.registerWorkflow(readWorkflowRunTx, { name: 'readWorkflowRunTx' });
const regStaticWorkflow = DBOS.registerWorkflow(staticWorkflow, { name: 'staticWorkflow' });
const regInstanceWorkflow = DBOS.registerWorkflow(instanceWorkflow, { name: 'instanceWorkflow' });
