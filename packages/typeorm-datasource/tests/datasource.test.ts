import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client, Pool } from 'pg';
import { TypeOrmDataSource } from '..';
import { dropDB, ensureDB } from './test-helpers';
import { randomUUID } from 'crypto';
import { SuperJSON } from 'superjson';
import { Column, DataSource, Entity, PrimaryColumn } from 'typeorm';

@Entity({ name: 'greetings' })
class Greeting {
  @PrimaryColumn()
  name: string = '';

  @Column()
  greet_count: number = 0;
}

const config = { user: 'postgres', database: 'typeorm_ds_test_datasource' };
const dataSource = TypeOrmDataSource.createFromConfig('app-db', config, [Greeting]);

interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

describe('TypeOrmDataSource', () => {
  const userDB = new Pool(config);

  beforeAll(async () => {
    await createDatabases(true);
  });

  afterAll(async () => {
    await userDB.end();
  });

  beforeEach(async () => {
    DBOS.setConfig({ name: 'typeorm-ds-test' });
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

  test('duplicate execution replays the winner result', async () => {
    await userDB.query('DELETE FROM race_side_effects');
    raceState.callCount = 0;

    // A loser whose body succeeds: the collision happens on the result-recording insert.
    raceState.plantWinner = true;
    raceState.plantWinnerError = false;
    raceState.fail = false;
    const wfid1 = randomUUID();
    await expect(DBOS.withNextWorkflowID(wfid1, () => regRaceWorkflow())).resolves.toBe('winner-result');
    expect(raceState.callCount).toBe(1); // the loser did run its body

    // A loser whose body fails: the collision moves to the error-recording insert,
    // and the winner's result still wins over the loser's own error.
    raceState.plantWinner = true;
    raceState.plantWinnerError = false;
    raceState.fail = true;
    const wfid2 = randomUUID();
    await expect(DBOS.withNextWorkflowID(wfid2, () => regRaceWorkflow())).resolves.toBe('winner-result');
    expect(raceState.callCount).toBe(2);

    // A winner that recorded an error: the loser replays that throw instead of its own result.
    raceState.plantWinner = true;
    raceState.plantWinnerError = true;
    raceState.fail = false;
    const wfid3 = randomUUID();
    const winnerError = await throws(() => DBOS.withNextWorkflowID(wfid3, () => regRaceWorkflow()));
    expect((winnerError as Error).message).toBe('winner-error');
    expect(raceState.callCount).toBe(3);

    // Every loser's writes were discarded and the winners' records still stand.
    const { rows: tags } = await userDB.query<{ tag: string }>('SELECT tag FROM race_side_effects');
    expect(tags).toHaveLength(0);
    const { rows: txOutput } = await userDB.query<transaction_completion>(
      'SELECT * FROM dbos.transaction_completion WHERE workflow_id = ANY($1)',
      [[wfid1, wfid2]],
    );
    expect(txOutput).toHaveLength(2);
    for (const row of txOutput) {
      expect(row.error).toBeNull(); // no loser error was ever recorded
      expect(SuperJSON.parse(row.output!)).toBe('winner-result');
    }
    const { rows: txError } = await userDB.query<transaction_completion>(
      'SELECT * FROM dbos.transaction_completion WHERE workflow_id = $1',
      [wfid3],
    );
    expect(txError).toHaveLength(1);
    expect(txError[0].output).toBeNull(); // the loser never overwrote the winner's error with its own result
    expect(SuperJSON.parse<Error>(txError[0].error!).message).toBe('winner-error');
  });
});

export interface greetings {
  name: string;
  greet_count: number;
}

async function createDatabases(createTxCompletions: boolean) {
  {
    const client = new Client({ ...config, database: 'postgres' });
    try {
      await client.connect();
      await dropDB(client, 'typeorm_ds_test', true);
      await dropDB(client, 'typeorm_ds_test_dbos_sys', true);
      await dropDB(client, config.database, true);
      await ensureDB(client, config.database);
    } finally {
      await client.end();
    }
  }

  if (createTxCompletions) {
    await TypeOrmDataSource.initializeDBOSSchema(config);
  }

  {
    const ds = new DataSource({
      type: 'postgres',
      username: config.user,
      database: config.database,
      entities: [Greeting],
    });
    await ds.initialize();
    await ds.synchronize();
    await ds.query('CREATE TABLE race_side_effects(tag text NOT NULL)');
    await ds.destroy();
  }
}

async function insertFunction(user: string) {
  type Result = Array<{ greet_count: number }>;
  const rows = await dataSource.entityManager.sql<Result>`
     INSERT INTO greetings(name, greet_count) VALUES(${user}, 1)
     ON CONFLICT(name) DO UPDATE SET greet_count = greetings.greet_count + 1
     RETURNING greet_count`;

  const row = rows.length > 0 ? rows[0] : undefined;
  return { user, greet_count: row?.greet_count, now: Date.now() };
}

async function errorFunction(user: string) {
  const _result = await insertFunction(user);
  throw new Error(`test error ${Date.now()}`);
}

const raceState = { callCount: 0, plantWinner: false, plantWinnerError: false, fail: false };

async function raceFunction() {
  raceState.callCount += 1;
  await dataSource.entityManager.sql`INSERT INTO race_side_effects(tag) VALUES (${`run-${raceState.callCount}`})`;
  if (raceState.plantWinner) {
    raceState.plantWinner = false;
    await plantWinnerCompletion();
  }
  if (raceState.fail) {
    throw new Error("loser's own failure");
  }
  return `result-${raceState.callCount}`;
}

// A duplicate execution commits its result while this transaction is still open, so this
// execution's pre-check missed it but its completion insert will collide with it.
async function plantWinnerCompletion() {
  const winner = new Client(config);
  try {
    await winner.connect();
    const [column, value] = raceState.plantWinnerError
      ? ['error', SuperJSON.stringify(new Error('winner-error'))]
      : ['output', SuperJSON.stringify('winner-result')];
    await winner.query(
      `INSERT INTO dbos.transaction_completion (workflow_id, function_num, ${column}) VALUES ($1, $2, $3)`,
      [DBOS.workflowID, DBOS.stepID, value],
    );
  } finally {
    await winner.end();
  }
}

const regRaceFunction = dataSource.registerTransaction(raceFunction);

async function raceWorkflow() {
  return await regRaceFunction();
}

const regRaceWorkflow = DBOS.registerWorkflow(raceWorkflow);

async function readFunction(user: string) {
  const result = await dataSource.entityManager.findOneBy(Greeting, { name: user });

  return { user, greet_count: result?.greet_count, now: Date.now() };
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

describe('TypeOrmDataSourceCreateTxC', () => {
  const userDB = new Pool(config);

  beforeAll(async () => {
    await createDatabases(false);
  });

  afterAll(async () => {
    await userDB.end();
  });

  beforeEach(async () => {
    DBOS.setConfig({ name: 'typeorm-ds-test' });
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
});
