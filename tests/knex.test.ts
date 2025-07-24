import request from 'supertest';

import { DBOS, Authentication, MiddlewareContext } from '../src';
import { DBOSInvalidWorkflowTransitionError, DBOSNotAuthorizedError } from '../src/error';
import { DBOSConfig } from '../src/dbos-executor';
import { UserDatabaseName } from '../src/user_database';
import { TestKvTable, generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { randomUUID } from 'node:crypto';
import { Knex } from 'knex';
import { DatabaseError } from 'pg';
import { EntityManager } from 'typeorm';

const testTableName = 'dbos_test_kv';

let insertCount = 0;

class TestClass {
  @DBOS.transaction()
  static async testInsert(value: string) {
    insertCount++;
    const result = await DBOS.knexClient<TestKvTable>(testTableName).insert({ value: value }).returning('id');
    return result[0].id!;
  }

  @DBOS.transaction()
  static async testSelect(id: number) {
    const result = await DBOS.knexClient<TestKvTable>(testTableName).select('value').where({ id: id });
    return result[0].value!;
  }

  @DBOS.workflow()
  static async testWf(value: string) {
    const id = await TestClass.testInsert(value);
    const result = await TestClass.testSelect(id);
    return result;
  }

  @DBOS.transaction()
  static async returnVoid() {}

  @DBOS.transaction()
  static async unsafeInsert(key: number, value: string) {
    insertCount++;
    const result = await DBOS.knexClient<TestKvTable>(testTableName).insert({ id: key, value: value }).returning('id');
    return result[0].id!;
  }

  @DBOS.transaction()
  static async nope1Txn() {
    await (DBOS.typeORMClient as EntityManager).query('Select * from t');
  }
}

describe('knex-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.KNEX);
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    insertCount = 0;
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-knex', async () => {
    await expect(TestClass.testInsert('test-one')).resolves.toBe(1);
    await expect(TestClass.testSelect(1)).resolves.toBe('test-one');
    await expect(TestClass.testWf('test-two')).resolves.toBe('test-two');

    await expect(TestClass.nope1Txn()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
  });

  test('knex-return-void', async () => {
    await expect(TestClass.returnVoid()).resolves.not.toThrow();
  });

  test('knex-duplicate-workflows', async () => {
    const uuid = randomUUID();
    const results = await Promise.allSettled([
      (await DBOS.startWorkflow(TestClass, { workflowID: uuid }).testWf('test-one')).getResult(),
      (await DBOS.startWorkflow(TestClass, { workflowID: uuid }).testWf('test-one')).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe('test-one');
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe('test-one');
    expect(insertCount).toBe(1);
  });

  test('knex-key-conflict', async () => {
    await TestClass.unsafeInsert(1, 'test-one');
    try {
      await TestClass.unsafeInsert(1, 'test-two');
      expect(true).toBe(false); // Fail if no error is thrown.
    } catch (e) {
      const err: DatabaseError = e as DatabaseError;
      expect(err.code).toBe('23505');
    }
  });
});

class TestEngine {
  static connectionString?: string;

  @DBOS.transaction()
  static async testEngine() {
    const ds = DBOS.knexClient;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
    expect((ds as any).context.client.connectionSettings.connectionString).toEqual(TestEngine.connectionString);
    await Promise.resolve();
  }
}

describe('knex-engine-config-tests', () => {
  test('engine-config', async () => {
    const config: DBOSConfig = {
      name: 'dbostest',
      userDatabaseClient: UserDatabaseName.KNEX,
      userDatabasePoolSize: 2,
      databaseUrl: `postgres://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/dbostest?connect_timeout=7`,
    };
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
    try {
      TestEngine.connectionString = config.databaseUrl;
      await TestEngine.testEngine();
    } finally {
      await DBOS.shutdown();
    }
  });
});
