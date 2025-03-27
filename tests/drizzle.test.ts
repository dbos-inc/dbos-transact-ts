import request from 'supertest';

import {
  Authentication,
  DBOS,
  GetApi,
  HandlerContext,
  MiddlewareContext,
  OrmEntities,
  PostApi,
  RequiredRole,
  Transaction,
  TransactionContext,
} from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { UserDatabaseName } from '../src/user_database';
import { generateDBOSTestConfig, generatePublicDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { pgTable, serial, text } from 'drizzle-orm/pg-core';
import { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { eq } from 'drizzle-orm';
import { v1 as uuidv1 } from 'uuid';
import { DatabaseError } from 'pg';
import { DBOSNotAuthorizedError } from '../src/error';

const testTableName = 'dbos_test_kv';

const testTable = pgTable(testTableName, {
  id: serial('id').primaryKey(),
  value: text('value'),
});

let insertCount = 0;

@OrmEntities()
export class NoEntities {}

@OrmEntities({ testTable })
class TestClass {
  @Transaction()
  static async testInsert(txnCtxt: TransactionContext<NodePgDatabase>, value: string) {
    insertCount++;
    const result = await txnCtxt.client.insert(testTable).values({ value }).returning({ id: testTable.id });
    return result[0].id;
  }

  @Transaction()
  static async testSelect(txnCtxt: TransactionContext<NodePgDatabase>, id: number) {
    const result = await txnCtxt.client.select().from(testTable).where(eq(testTable.id, id));
    return result[0].value;
  }

  @Transaction()
  static async testQuery(ctx: TransactionContext<NodePgDatabase<{ testTable: typeof testTable }>>) {
    const result = await ctx.client.query.testTable.findMany();
    return result[0].value;
  }

  @DBOS.workflow()
  static async testWf(value: string) {
    const id = await DBOS.invoke(TestClass).testInsert(value);
    const result = await DBOS.invoke(TestClass).testSelect(id);
    return result;
  }

  @Transaction()
  static async returnVoid(_ctxt: TransactionContext<NodePgDatabase>) {}

  @Transaction()
  static async unsafeInsert(txnCtxt: TransactionContext<NodePgDatabase>, key: number, value: string) {
    insertCount++;
    const result = await txnCtxt.client.insert(testTable).values({ id: key, value }).returning({ id: testTable.id });
    return result[0].id;
  }
}

describe('drizzle-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.DRIZZLE);
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

  test('simple-drizzle', async () => {
    await expect(DBOS.invoke(TestClass).testInsert('test-one')).resolves.toBe(1);
  });

  test('drizzle-query', async () => {
    await DBOS.invoke(TestClass).testInsert('test-query');
    await expect(DBOS.invoke(TestClass).testQuery()).resolves.toBe('test-query');
  });

  test('drizzle-return-void', async () => {
    await expect(DBOS.invoke(TestClass).returnVoid()).resolves.not.toThrow();
  });

  test('drizzle-duplicate-workflows', async () => {
    const uuid = uuidv1();
    const results = await Promise.allSettled([
      (await DBOS.startWorkflow(TestClass, { workflowID: uuid }).testWf('test-one')).getResult(),
      (await DBOS.startWorkflow(TestClass, { workflowID: uuid }).testWf('test-one')).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe('test-one');
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe('test-one');
    expect(insertCount).toBe(1);
  });

  test('drizzle-key-conflict', async () => {
    await DBOS.invoke(TestClass).unsafeInsert(1, 'test-one');
    try {
      await DBOS.invoke(TestClass).unsafeInsert(1, 'test-two');
      expect(true).toBe(false); // Fail if no error is thrown.
    } catch (e) {
      const err: DatabaseError = e as DatabaseError;
      expect(err.code).toBe('23505');
    }
  });
});

const userTableName = 'dbos_test_user';

const userTable = pgTable(userTableName, {
  id: serial('id').primaryKey(),
  username: text('username'),
});

@Authentication(DUserManager.authMiddlware)
export class DUserManager {
  @Transaction()
  @PostApi('/register')
  static async createUser(ctx: TransactionContext<NodePgDatabase>, uname: string) {
    return await ctx.client.insert(userTable).values({ username: uname }).returning({ id: userTable.id });
  }

  @GetApi('/hello')
  @RequiredRole(['user'])
  static async hello(hCtxt: HandlerContext) {
    return Promise.resolve({ messge: 'hello ' + hCtxt.authenticatedUser });
  }

  static async authMiddlware(ctx: MiddlewareContext) {
    if (!ctx.requiredRole || !ctx.requiredRole.length) {
      return;
    }
    const { user } = ctx.koaContext.query;
    if (!user) {
      throw new DBOSNotAuthorizedError('User not provided', 401);
    }
    const u = await ctx.query(async (c: NodePgDatabase, uname: string) => {
      return await c.select().from(userTable).where(eq(userTable.username, uname));
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

describe('drizzle-auth-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.DRIZZLE);
    await setUpDBOSTestDb(config);
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

  test('simple-drizzle-auth', async () => {
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
    const ds = DBOS.drizzleClient;
    expect((ds as any).session.client._connectionTimeoutMillis).toEqual(3000);
    // Drizzle doesn't expose the pool directly
  }
}

describe('typeorm-engine-config-tests', () => {
  test('engine-config', async () => {
    const config = generatePublicDBOSTestConfig({
      userDbclient: UserDatabaseName.DRIZZLE,
      userDbPoolSize: 2,
    });
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
    await TestEngine.testEngine();
    await DBOS.shutdown();
  });
});
