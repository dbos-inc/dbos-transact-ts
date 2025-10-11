import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client, Pool } from 'pg';
import { KyselyDataSource } from '..';
import { dropDB, ensureDB, getKyselyDB, GreetingsTable } from './test-helpers';
import { randomUUID } from 'crypto';
import SuperJSON from 'superjson';
import { sql } from 'kysely';

interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

const pgConfig = { user: 'postgres', database: 'kysely_ds_test_userdb' };
const db = getKyselyDB('kysely_ds_test_userdb');
const dataSource = new KyselyDataSource('app-db', db);

describe.only('KyselyDataSource', () => {
  const userDB = new Pool(pgConfig);

  beforeAll(async () => {
    await createDatabases(true);
  });

  afterAll(async () => {
    await userDB.end();
  });

  beforeEach(async () => {
    DBOS.setConfig({ name: 'kysely-ds-test' });
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

    const { rows } = await userDB.query<GreetingsTable>('SELECT * FROM greetings WHERE name = $1', [user]);
    expect(rows.length).toBe(1);
    expect(rows[0].greet_count).toBe(10);

    const { rows: txOutput } = await userDB.query<transaction_completion>(
      'SELECT * FROM dbos.transaction_completion WHERE workflow_id = $1',
      [workflowID],
    );

    console.log({ txOutput });
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

    const { rows } = await userDB.query<GreetingsTable>('SELECT * FROM greetings WHERE name = $1', [user]);
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

async function createDatabases(createTxCompletion: boolean) {
  {
    const client = new Client({ ...pgConfig, database: 'postgres' });
    try {
      await client.connect();
      await dropDB(client, pgConfig.database, true);
      await ensureDB(client, pgConfig.database);
    } finally {
      await client.end();
    }
  }

  {
    const pool = new Pool(pgConfig);
    const client = await pool.connect();
    try {
      await client.query(
        'CREATE TABLE greetings(name text NOT NULL PRIMARY KEY, greet_count integer DEFAULT 0 NOT NULL)',
      );
    } finally {
      client.release();
      await pool.end();
    }
  }

  if (createTxCompletion) {
    await KyselyDataSource.initializeDBOSSchema(db);
  }
}

async function insertFunction(user: string) {
  const result = await dataSource.client
    .insertInto('greetings')
    .values({ name: user, greet_count: 1 })
    .onConflict((oc) => oc.column('name').doUpdateSet({ greet_count: sql`greetings.greet_count + 1` }))
    .returning('greet_count')
    .executeTakeFirst();

  return { user, greet_count: result?.greet_count, now: Date.now() };
}

async function errorFunction(user: string) {
  await insertFunction(user);
  throw new Error(`test error ${Date.now()}`);
}

async function readFunction(user: string) {
  const row = await dataSource.client
    .selectFrom('greetings')
    .select('greet_count')
    .where('name', '=', user)
    .executeTakeFirst();
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

const regInsertWorkflowReg = DBOS.registerWorkflow(insertWorkflowReg);
const regInsertWorkflowRunTx = DBOS.registerWorkflow(insertWorkflowRunTx);
const regErrorWorkflowReg = DBOS.registerWorkflow(errorWorkflowReg);
const regErrorWorkflowRunTx = DBOS.registerWorkflow(errorWorkflowRunTx);
const regReadWorkflowReg = DBOS.registerWorkflow(readWorkflowReg);
const regReadWorkflowRunTx = DBOS.registerWorkflow(readWorkflowRunTx);
const regStaticWorkflow = DBOS.registerWorkflow(staticWorkflow);
const regInstanceWorkflow = DBOS.registerWorkflow(instanceWorkflow);

describe('KyselyDataSourceCreateTxCompletion', () => {
  const userDB = new Pool(pgConfig);

  beforeAll(async () => {
    await createDatabases(false);
  });

  afterAll(async () => {
    await userDB.end();
  });

  beforeEach(async () => {
    DBOS.setConfig({ name: 'kysely-ds-test' });
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('run wf auto register DS schema', async () => {
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
});
