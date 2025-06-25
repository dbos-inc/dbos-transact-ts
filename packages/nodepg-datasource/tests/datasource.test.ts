import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client, Pool } from 'pg';
import { NodePostgresDataSource } from '..';
import { dropDB, ensureDB } from './test-helpers';
import { randomUUID } from 'crypto';
import { SuperJSON } from 'superjson';

const config = { user: 'postgres', database: 'node_pg_ds_test_datasource' };
const dataSource = new NodePostgresDataSource('app-db', config);

interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

describe('NodePostgresDataSource', () => {
  const userDB = new Pool(config);

  beforeAll(async () => {
    {
      const client = new Client({ ...config, database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'node_pg_ds_test', true);
        await dropDB(client, 'node_pg_ds_test_dbos_sys', true);
        await dropDB(client, config.database, true);
        await ensureDB(client, config.database);
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

    await NodePostgresDataSource.initializeDBOSSchema(config);
  });

  afterAll(async () => {
    await userDB.end();
  });

  beforeEach(async () => {
    DBOS.setConfig({ name: 'node-pg-ds-test' });
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('insert dataSource.register function', async () => {
    const user = 'helloTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regInsertWorkflowReg(user))).resolves.toMatchObject({
      user,
      greet_count: 1,
    });

    const { rows } = await userDB.query<transaction_completion>(
      'SELECT * FROM dbos.transaction_completion WHERE workflow_id = $1',
      [workflowID],
    );
    expect(rows.length).toBe(1);
    expect(rows[0].workflow_id).toBe(workflowID);
    expect(rows[0].function_num).toBe(0);
    expect(rows[0].output).not.toBeNull();
    expect(SuperJSON.parse(rows[0].output!)).toMatchObject({ user, greet_count: 1 });
  });

  test('rerun insert dataSource.register function', async () => {
    const user = 'rerunTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    const result = await DBOS.withNextWorkflowID(workflowID, () => regInsertWorkflowReg(user));
    expect(result).toMatchObject({ user, greet_count: 1 });

    await expect(DBOS.withNextWorkflowID(workflowID, () => regInsertWorkflowReg(user))).resolves.toMatchObject(result);
  });

  test('insert dataSource.runAsTx function', async () => {
    const user = 'helloTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regInsertWorkflowRunTx(user))).resolves.toMatchObject({
      user,
      greet_count: 1,
    });

    const { rows } = await userDB.query<transaction_completion>(
      'SELECT * FROM dbos.transaction_completion WHERE workflow_id = $1',
      [workflowID],
    );
    expect(rows.length).toBe(1);
    expect(rows[0].workflow_id).toBe(workflowID);
    expect(rows[0].function_num).toBe(0);
    expect(rows[0].output).not.toBeNull();
    expect(SuperJSON.parse(rows[0].output!)).toMatchObject({ user, greet_count: 1 });
  });

  test('rerun insert dataSource.runAsTx function', async () => {
    const user = 'rerunTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    const result = await DBOS.withNextWorkflowID(workflowID, () => regInsertWorkflowRunTx(user));
    expect(result).toMatchObject({ user, greet_count: 1 });

    await expect(DBOS.withNextWorkflowID(workflowID, () => regInsertWorkflowRunTx(user))).resolves.toMatchObject(
      result,
    );
  });

  async function throws<R>(func: () => Promise<R>): Promise<unknown> {
    try {
      await func();
      fail('Expected function to throw an error');
    } catch (error) {
      return error;
    }
  }

  test('error dataSource.register function', async () => {
    const user = 'errorTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);
    const workflowID = randomUUID();

    const error = await throws(() => DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowReg(user)));
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(/^test error \d+$/);

    const { rows } = await userDB.query<greetings>('SELECT * FROM greetings WHERE name = $1', [user]);
    expect(rows.length).toBe(1);
    expect(rows[0].greet_count).toBe(10);

    const { rows: txOutput } = await userDB.query<transaction_completion>(
      'SELECT * FROM dbos.transaction_completion WHERE workflow_id = $1',
      [workflowID],
    );
    expect(txOutput.length).toBe(1);
    expect(txOutput[0].workflow_id).toBe(workflowID);
    expect(txOutput[0].function_num).toBe(0);
    expect(txOutput[0].output).toBeNull();
    expect(txOutput[0].error).not.toBeNull();
    const $error = SuperJSON.parse(txOutput[0].error!);
    expect($error).toBeInstanceOf(Error);
    expect(($error as Error).message).toMatch(/^test error \d+$/);
  });

  test('rerun error dataSource.register function', async () => {
    const user = 'rerunErrorTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);
    const workflowID = randomUUID();

    const error = await throws(() => DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowReg(user)));
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(/^test error \d+$/);

    const error2 = await throws(() => DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowReg(user)));
    expect(error2).toBeInstanceOf(Error);
    expect((error2 as Error).message).toMatch((error as Error).message);
  });

  test('error dataSource.runAsTx function', async () => {
    const user = 'errorTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);
    const workflowID = randomUUID();

    const error = await throws(() => DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowRunTx(user)));
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(/^test error \d+$/);

    const { rows } = await userDB.query<greetings>('SELECT * FROM greetings WHERE name = $1', [user]);
    expect(rows.length).toBe(1);
    expect(rows[0].greet_count).toBe(10);

    const { rows: txOutput } = await userDB.query<transaction_completion>(
      'SELECT * FROM dbos.transaction_completion WHERE workflow_id = $1',
      [workflowID],
    );
    expect(txOutput.length).toBe(1);
    expect(txOutput[0].workflow_id).toBe(workflowID);
    expect(txOutput[0].function_num).toBe(0);
    expect(txOutput[0].output).toBeNull();
    expect(txOutput[0].error).not.toBeNull();
    const $error = SuperJSON.parse(txOutput[0].error!);
    expect($error).toBeInstanceOf(Error);
    expect(($error as Error).message).toMatch(/^test error \d+$/);
  });

  test('rerun error dataSource.runAsTx function', async () => {
    const user = 'rerunErrorTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);
    const workflowID = randomUUID();

    const error = await throws(() => DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowRunTx(user)));
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(/^test error \d+$/);

    const error2 = await throws(() => DBOS.withNextWorkflowID(workflowID, () => regErrorWorkflowRunTx(user)));
    expect(error2).toBeInstanceOf(Error);
    expect((error2 as Error).message).toMatch((error as Error).message);
  });

  test('readonly dataSource.register function', async () => {
    const user = 'readTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regReadWorkflowReg(user))).resolves.toMatchObject({
      user,
      greet_count: 10,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_completion WHERE workflow_id = $1', [
      workflowID,
    ]);
    expect(rows.length).toBe(0);
  });

  test('readonly dataSource.runAsTx function', async () => {
    const user = 'readTest2';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);
    await userDB.query('INSERT INTO greetings("name","greet_count") VALUES($1,10);', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regReadWorkflowRunTx(user))).resolves.toMatchObject({
      user,
      greet_count: 10,
    });

    const { rows } = await userDB.query('SELECT * FROM dbos.transaction_completion WHERE workflow_id = $1', [
      workflowID,
    ]);
    expect(rows.length).toBe(0);
  });

  test('static dataSource.register methods', async () => {
    const user = 'staticTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regStaticWorkflow(user))).resolves.toMatchObject([
      { user, greet_count: 1 },
      { user, greet_count: 1 },
    ]);
  });

  test('instance dataSource.register methods', async () => {
    const user = 'instanceTest1';

    await userDB.query('DELETE FROM greetings WHERE name = $1', [user]);

    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => regInstanceWorkflow(user))).resolves.toMatchObject([
      { user, greet_count: 1 },
      { user, greet_count: 1 },
    ]);
  });

  test('invoke-reg-tx-fun-outside-wf', async () => {
    const user = 'outsideWfUser' + Date.now();
    const result = await regInsertFunction(user);
    expect(result).toMatchObject({ user, greet_count: 1 });

    const txResults = await userDB.query('SELECT * FROM dbos.transaction_completion WHERE output LIKE $1', [
      `%${user}%`,
    ]);
    expect(txResults.rows.length).toBe(0);
  });

  test('invoke-reg-tx-static-method-outside-wf', async () => {
    const user = 'outsideWfUser' + Date.now();
    const result = await StaticClass.insertFunction(user);
    expect(result).toMatchObject({ user, greet_count: 1 });

    const txResults = await userDB.query('SELECT * FROM dbos.transaction_completion WHERE output LIKE $1', [
      `%${user}%`,
    ]);
    expect(txResults.rows.length).toBe(0);
  });

  test('invoke-reg-tx-inst-method-outside-wf', async () => {
    const user = 'outsideWfUser' + Date.now();
    const instance = new InstanceClass();
    const result = await instance.insertFunction(user);
    expect(result).toMatchObject({ user, greet_count: 1 });

    const txResults = await userDB.query('SELECT * FROM dbos.transaction_completion WHERE output LIKE $1', [
      `%${user}%`,
    ]);
    expect(txResults.rows.length).toBe(0);
  });
});

export interface greetings {
  name: string;
  greet_count: number;
}

async function insertFunction(user: string) {
  const { rows } = await NodePostgresDataSource.client.query<Pick<greetings, 'greet_count'>>(
    `INSERT INTO greetings(name, greet_count) 
     VALUES($1, 1) 
     ON CONFLICT(name)
     DO UPDATE SET greet_count = greetings.greet_count + 1
     RETURNING greet_count`,
    [user],
  );
  const row = rows.length > 0 ? rows[0] : undefined;
  return { user, greet_count: row?.greet_count, now: Date.now() };
}

async function errorFunction(user: string) {
  const _result = await insertFunction(user);
  throw new Error(`test error ${Date.now()}`);
}

async function readFunction(user: string) {
  const { rows } = await dataSource.client.query<Pick<greetings, 'greet_count'>>(
    `SELECT greet_count
     FROM greetings
     WHERE name = $1`,
    [user],
  );
  const row = rows.length > 0 ? rows[0] : undefined;
  return { user, greet_count: row?.greet_count, now: Date.now() };
}

const regInsertFunction = dataSource.registerTransaction(insertFunction);
const regErrorFunction = dataSource.registerTransaction(errorFunction);
const regReadFunction = dataSource.registerTransaction(readFunction, { readOnly: true });

class StaticClass {
  static async insertFunction(user: string) {
    return await insertFunction(user);
  }

  static async readFunction(user: string) {
    return await readFunction(user);
  }
}

StaticClass.insertFunction = dataSource.registerTransaction(StaticClass.insertFunction);
StaticClass.readFunction = dataSource.registerTransaction(StaticClass.readFunction, { readOnly: true });

class InstanceClass {
  async insertFunction(user: string) {
    return await insertFunction(user);
  }

  async readFunction(user: string) {
    return await readFunction(user);
  }
}

InstanceClass.prototype.insertFunction = dataSource.registerTransaction(
  // eslint-disable-next-line @typescript-eslint/unbound-method
  InstanceClass.prototype.insertFunction,
);
InstanceClass.prototype.readFunction = dataSource.registerTransaction(
  // eslint-disable-next-line @typescript-eslint/unbound-method
  InstanceClass.prototype.readFunction,
  { readOnly: true },
);

async function insertWorkflowReg(user: string) {
  return await regInsertFunction(user);
}

async function insertWorkflowRunTx(user: string) {
  return await dataSource.runTransaction(() => insertFunction(user), { name: 'insertFunction' });
}

async function errorWorkflowReg(user: string) {
  return await regErrorFunction(user);
}

async function errorWorkflowRunTx(user: string) {
  return await dataSource.runTransaction(() => errorFunction(user), { name: 'errorFunction' });
}

async function readWorkflowReg(user: string) {
  return await regReadFunction(user);
}

async function readWorkflowRunTx(user: string) {
  return await dataSource.runTransaction(() => readFunction(user), { name: 'readFunction', readOnly: true });
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

const regInsertWorkflowReg = DBOS.registerWorkflow(insertWorkflowReg, 'insertWorkflowReg');
const regInsertWorkflowRunTx = DBOS.registerWorkflow(insertWorkflowRunTx, 'insertWorkflowRunTx');
const regErrorWorkflowReg = DBOS.registerWorkflow(errorWorkflowReg, 'errorWorkflowReg');
const regErrorWorkflowRunTx = DBOS.registerWorkflow(errorWorkflowRunTx, 'errorWorkflowRunTx');
const regReadWorkflowReg = DBOS.registerWorkflow(readWorkflowReg, 'readWorkflowReg');
const regReadWorkflowRunTx = DBOS.registerWorkflow(readWorkflowRunTx, 'readWorkflowRunTx');
const regStaticWorkflow = DBOS.registerWorkflow(staticWorkflow, 'staticWorkflow');
const regInstanceWorkflow = DBOS.registerWorkflow(instanceWorkflow, 'instanceWorkflow');
