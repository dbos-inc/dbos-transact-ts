import request from 'supertest';

import { Authentication, DBOS, MiddlewareContext, OrmEntities } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { UserDatabaseName } from '../src/user_database';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { pgTable, serial, text, integer, varchar } from 'drizzle-orm/pg-core';
import { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { eq } from 'drizzle-orm';
import { randomUUID } from 'node:crypto';
import { DatabaseError } from 'pg';
import { DBOSNotAuthorizedError } from '../src/error';
import { sleepms } from '../src/utils';
import { IsolationLevel } from '../src/transaction';

const testTableName = 'dbos_test_kv';

const testTable = pgTable(testTableName, {
  id: serial('id').primaryKey(),
  value: text('value'),
});

const countersTable = pgTable('counters', {
  id: serial('id').primaryKey(),
  name: varchar('name', { length: 255 }),
  value: integer('value').notNull().default(0),
});

let insertCount = 0;

@OrmEntities()
export class NoEntities {}

type DrizzleDB = NodePgDatabase<{ testTable: typeof testTable; counters: typeof countersTable }>;
const getDrizzleDB = () => DBOS.drizzleClient as DrizzleDB;

@OrmEntities({ testTable, countersDb: countersTable })
class TestClass {
  @DBOS.transaction()
  static async testInsert(value: string) {
    insertCount++;
    const result = await getDrizzleDB().insert(testTable).values({ value }).returning({ id: testTable.id });
    return result[0].id;
  }

  @DBOS.transaction()
  static async testSelect(id: number) {
    const result = await getDrizzleDB().select().from(testTable).where(eq(testTable.id, id));
    return result[0].value;
  }

  @DBOS.transaction()
  static async testQuery() {
    const result = await getDrizzleDB().query.testTable.findMany();
    return result[0].value;
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
    const result = await getDrizzleDB().insert(testTable).values({ id: key, value }).returning({ id: testTable.id });
    return result[0].id;
  }

  static conflictTrigger: Promise<void> | undefined = undefined;

  @DBOS.transaction({ isolationLevel: IsolationLevel.Serializable })
  static async testSerzConflict() {
    const res = await getDrizzleDB().select().from(countersTable).where(eq(countersTable.name, 'conflict'));
    const value = res[0].value;

    await TestClass.conflictTrigger; // simulate work

    console.log(`Setting value from ${value} => ${value + 1} in ${DBOS.workflowID}`);
    await getDrizzleDB()
      .update(countersTable)
      .set({ value: value + 1 })
      .where(eq(countersTable.name, 'conflict'));

    return value + 1;
  }
  @DBOS.workflow()
  static async testSerzXflictWF() {
    return await TestClass.testSerzConflict();
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

    await DBOS.queryUserDB('DROP TABLE IF EXISTS counters');
    await DBOS.queryUserDB(`
      CREATE TABLE counters (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        value INTEGER NOT NULL DEFAULT 0
      )
    `);
    await DBOS.queryUserDB(`INSERT INTO counters (name, value) VALUES ('conflict', 0)`);

    insertCount = 0;
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-drizzle', async () => {
    await expect(TestClass.testInsert('test-one')).resolves.toBe(1);
  });

  test('drizzle-query', async () => {
    await TestClass.testInsert('test-query');
    await expect(TestClass.testQuery()).resolves.toBe('test-query');
  });

  test('drizzle-return-void', async () => {
    await expect(TestClass.returnVoid()).resolves.not.toThrow();
  });

  test('drizzle-duplicate-workflows', async () => {
    const uuid = randomUUID();
    const results = await Promise.allSettled([
      (await DBOS.startWorkflow(TestClass, { workflowID: uuid }).testWf('test-one')).getResult(),
      (await DBOS.startWorkflow(TestClass, { workflowID: uuid }).testWf('test-one')).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe('test-one');
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe('test-one');
    expect(insertCount).toBe(1);
  });

  test('drizzle-key-conflict', async () => {
    await TestClass.unsafeInsert(1, 'test-one');
    try {
      await TestClass.unsafeInsert(1, 'test-two');
      expect(true).toBe(false); // Fail if no error is thrown.
    } catch (e) {
      const err: DatabaseError = e as DatabaseError;
      expect(err.code).toBe('23505');
    }
  });

  test('drizzle-serz-xflict', async () => {
    let cb: () => void | undefined;
    TestClass.conflictTrigger = new Promise((ressolve) => {
      cb = ressolve;
    });

    // Start 2 conflicting transactions; they will wedge on the Promise
    const h1 = await DBOS.startWorkflow(TestClass).testSerzXflictWF();
    const h2 = await DBOS.startWorkflow(TestClass).testSerzXflictWF();

    await sleepms(100);
    cb!(); // Resolve Promise

    const r1 = await h1.getResult();
    const r2 = await h2.getResult();
    expect(r1 + r2).toBe(3); // These should have gone as if sequentially
  });
});

const userTableName = 'dbos_test_user';

const userTable = pgTable(userTableName, {
  id: serial('id').primaryKey(),
  username: text('username'),
});

@Authentication(DUserManager.authMiddlware)
export class DUserManager {
  @DBOS.transaction()
  @DBOS.postApi('/register')
  static async createUser(uname: string) {
    return await getDrizzleDB().insert(userTable).values({ username: uname }).returning({ id: userTable.id });
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
    const _pc = DBOS.dbosConfig?.poolConfig;
    const _ds = DBOS.drizzleClient;
    // expect((ds as any).session.client._connectionTimeoutMillis).toEqual(pc?.connectionTimeoutMillis);
    // Drizzle doesn't expose the pool directly
    await Promise.resolve();
  }
}

describe('drizzle-engine-config-tests', () => {
  test('engine-config', async () => {
    const config: DBOSConfig = {
      name: 'dbostest',
      userDbClient: UserDatabaseName.DRIZZLE,
      userDbPoolSize: 2,
      databaseUrl: `postgres://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/dbostest?connect_timeout=7`,
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
