import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { KnexDataSource } from '../index';
import { dropDB } from './test-helpers';
import { randomUUID } from 'crypto';
import sqlite3 from 'sqlite3';
import fs from 'fs/promises';
import path from 'path';

const config = {
  client: 'sqlite3',
  useNullAsDefault: true,
  connection: {
    filename: path.join(__dirname, '../dbos-test.sqlite'),
  },
};

const dataSource = new KnexDataSource('app-db', config);
DBOS.registerDataSource(dataSource);

interface transaction_outputs {
  workflow_id: string;
  function_num: number;
  output: string | null;
}

function run(db: sqlite3.Database, sql: string, params: any[] = []): Promise<void> {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

function all<T>(db: sqlite3.Database, sql: string, params: any[] = []): Promise<T[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) {
        reject(err);
      } else {
        resolve(rows as T[]);
      }
    });
  });
}

describe('SQLite KnexDataSource', () => {
  let db: sqlite3.Database | undefined;

  beforeAll(async () => {
    const exists = await fs.stat(config.connection.filename).then(
      () => true,
      () => false,
    );
    if (exists) {
      await fs.rm(config.connection.filename);
    }

    const client = new Client({ user: 'postgres', database: 'postgres' });
    try {
      await client.connect();
      await dropDB(client, 'knex_sqlite_test');
      await dropDB(client, 'knex_sqlite_test_dbos_sys');
    } finally {
      await client.end();
    }

    db = new sqlite3.Database(config.connection.filename);
    await run(db, 'CREATE TABLE greetings(name TEXT NOT NULL, greet_count INTEGER DEFAULT 0, PRIMARY KEY(name))');
    await KnexDataSource.configure(config);
    DBOS.setConfig({ name: 'knex-sqlite-test' });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
    db?.close();
  });

  test('insert dataSource.register function', async () => {
    const user = 'helloTest1';

    await run(db!, 'DELETE FROM greetings WHERE name = ?', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regInsertWorfklowReg(user))).resolves.toEqual({
      user,
      greet_count: 1,
    });

    const rows = await all<transaction_outputs>(db!, 'SELECT * FROM dbos_transaction_outputs WHERE workflow_id = $1', [
      workflowID,
    ]);
    expect(rows.length).toBe(1);
    expect(rows[0].workflow_id).toBe(workflowID);
    expect(rows[0].function_num).toBe(0);
    expect(rows[0].output).not.toBeNull();
    expect(JSON.parse(rows[0].output!)).toEqual({ user, greet_count: 1 });
  });

  test('insert dataSource.runAsTx function', async () => {
    const user = 'helloTest2';

    await run(db!, 'DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regInsertWorfklowRunTx(user))).resolves.toEqual({
      user,
      greet_count: 1,
    });

    const rows = await all<transaction_outputs>(db!, 'SELECT * FROM dbos_transaction_outputs WHERE workflow_id = $1', [
      workflowID,
    ]);
    expect(rows.length).toBe(1);
    expect(rows[0].workflow_id).toBe(workflowID);
    expect(rows[0].function_num).toBe(0);
    expect(rows[0].output).not.toBeNull();
    expect(JSON.parse(rows[0].output!)).toEqual({ user, greet_count: 1 });
  });

  test('error dataSource.register function', async () => {
    const user = 'errorTest1';

    await run(db!, 'DELETE FROM greetings WHERE name = $1', [user]);
    await run(db!, 'INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowReg(user))).rejects.toThrow('test error');

    const txOutput = await all<transaction_outputs>(
      db!,
      'SELECT * FROM dbos_transaction_outputs WHERE workflow_id = $1',
      [workflowID],
    );
    expect(txOutput.length).toBe(0);

    const rows = await all<greetings>(db!, 'SELECT * FROM greetings WHERE name = $1', [user]);
    expect(rows.length).toBe(1);
    expect(rows[0].greet_count).toBe(10);
  });

  test('error dataSource.runAsTx function', async () => {
    const user = 'errorTest2';

    await run(db!, 'DELETE FROM greetings WHERE name = $1', [user]);
    await run(db!, 'INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowRunTx(user))).rejects.toThrow('test error');

    const txOutput = await all<transaction_outputs>(
      db!,
      'SELECT * FROM dbos_transaction_outputs WHERE workflow_id = $1',
      [workflowID],
    );
    expect(txOutput.length).toBe(0);

    const rows = await all<greetings>(db!, 'SELECT * FROM greetings WHERE name = $1', [user]);
    expect(rows.length).toBe(1);
    expect(rows[0].greet_count).toBe(10);
  });

  test('readonly dataSource.register function', async () => {
    const user = 'readTest1';

    await run(db!, 'DELETE FROM greetings WHERE name = $1', [user]);
    await run(db!, 'INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regReadWorkflowReg(user))).resolves.toEqual({
      user,
      greet_count: 10,
    });

    const rows = await all<transaction_outputs>(db!, 'SELECT * FROM dbos_transaction_outputs WHERE workflow_id = $1', [
      workflowID,
    ]);
    expect(rows.length).toBe(0);
  });

  test('readonly dataSource.runAsTx function', async () => {
    const user = 'readTest2';

    await run(db!, 'DELETE FROM greetings WHERE name = $1', [user]);
    await run(db!, 'INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regReadWorkflowRunTx(user))).resolves.toEqual({
      user,
      greet_count: 10,
    });

    const rows = await all<transaction_outputs>(db!, 'SELECT * FROM dbos_transaction_outputs WHERE workflow_id = $1', [
      workflowID,
    ]);
    expect(rows.length).toBe(0);
  });

  test('static dataSource.register methods', async () => {
    const user = 'staticTest1';

    await run(db!, 'DELETE FROM greetings WHERE name = $1', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regStaticWorkflow(user))).resolves.toEqual([
      { user, greet_count: 1 },
      { user, greet_count: 1 },
    ]);
  });

  test('instance dataSource.register methods', async () => {
    const user = 'instanceTest1';

    await run(db!, 'DELETE FROM greetings WHERE name = $1', [user]);

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

// eslint-disable-next-line @typescript-eslint/unbound-method
InstanceClass.prototype.insertFunction = dataSource.register(InstanceClass.prototype.insertFunction, 'insertFunction');
// eslint-disable-next-line @typescript-eslint/unbound-method
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
