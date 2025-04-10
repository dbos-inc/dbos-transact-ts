import request from 'supertest';

import { DBOS, Authentication, MiddlewareContext } from '../src';
import { DBOSInvalidWorkflowTransitionError, DBOSNotAuthorizedError } from '../src/error';
import { DBOSConfig, DBOSConfigInternal } from '../src/dbos-executor';
import { UserDatabaseName } from '../src/user_database';
import { TestKvTable, generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { v1 as uuidv1 } from 'uuid';
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
    const uuid = uuidv1();
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

const userTableName = 'dbos_test_user';
interface UserTable {
  id?: number;
  username?: string;
}

@Authentication(KUserManager.authMiddlware)
class KUserManager {
  @DBOS.transaction()
  @DBOS.postApi('/register')
  static async createUser(uname: string) {
    const result = await DBOS.knexClient<UserTable>(userTableName).insert({ username: uname }).returning('id');
    return result;
  }

  @DBOS.getApi('/hello')
  @DBOS.requiredRole(['user'])
  static async hello() {
    return Promise.resolve({ messge: 'hello ' + DBOS.authenticatedUser });
  }

  static async authMiddlware(ctx: MiddlewareContext) {
    if (!ctx.requiredRole || !ctx.requiredRole.length) {
      return;
    }
    const { user } = ctx.koaContext.query;
    if (!user) {
      throw new DBOSNotAuthorizedError('User not provided', 401);
    }
    const u = await ctx.query((dbClient: Knex, uname: string) => {
      return dbClient<UserTable>(userTableName).select('username').where({ username: uname });
    }, user as string);

    if (!u || !u.length) {
      throw new DBOSNotAuthorizedError('User does not exist', 403);
    }
    ctx.logger.info(`Allowed in user: ${u[0].username}`);
    return {
      authenticatedUser: u[0].username!,
      authenticatedRoles: ['user'],
    };
  }
}

describe('knex-auth-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.KNEX);
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    DBOS.setUpHandlerCallback();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${userTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${userTableName} (id SERIAL PRIMARY KEY, username TEXT);`);
  });

  afterEach(async () => {
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${userTableName};`);
    await DBOS.shutdown();
  });

  test('simple-knex-auth', async () => {
    // No user name
    const response1 = await request(DBOS.getHTTPHandlersCallback()!).get('/hello');
    expect(response1.statusCode).toBe(401);

    // User name doesn't exist
    const response2 = await request(DBOS.getHTTPHandlersCallback()!).get('/hello?user=paul');
    expect(response2.statusCode).toBe(403);

    const response3 = await request(DBOS.getHTTPHandlersCallback()!).post('/register').send({ uname: 'paul' });
    expect(response3.statusCode).toBe(200);

    const response4 = await request(DBOS.getHTTPHandlersCallback()!).get('/hello?user=paul');
    expect(response4.statusCode).toBe(200);
  });
});

class TestEngine {
  @DBOS.transaction()
  static async testEngine() {
    const pc = (DBOS.dbosConfig as DBOSConfigInternal).poolConfig;
    const ds = DBOS.knexClient;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
    expect((ds as any).context.client.config.connection.connectionTimeoutMillis).toEqual(3000);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
    expect((ds as any).context.client.config.connection.ssl).toEqual(false);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
    expect((ds as any).context.client.config.connection.connectionString).toEqual(pc.connectionString);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
    expect((ds as any).context.client.config.pool.max).toEqual(2);
    await Promise.resolve();
  }
}

describe('knex-engine-config-tests', () => {
  test('engine-config', async () => {
    const config = {
      name: 'dbostest',
      userDbclient: UserDatabaseName.KNEX,
      userDbPoolSize: 2,
    };
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
    try {
      await TestEngine.testEngine();
    } finally {
      await DBOS.shutdown();
    }
  });
});
