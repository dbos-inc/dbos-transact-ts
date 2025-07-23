import request from 'supertest';

import { PrismaClient, testkv } from '@prisma/client';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { Authentication, MiddlewareContext, DBOS } from '../src';
import { DBOSNotAuthorizedError } from '../src/error';

import { randomUUID } from 'node:crypto';
import { sleepms } from '../src/utils';
import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import { UserDatabaseName } from '../src/user_database';
import { DBOSConfig } from '../src/dbos-executor';

interface PrismaPGError {
  code: string;
  meta: {
    code: string;
    message: string;
  };
}

/**
 * Funtions used in tests.
 */
let globalCnt = 0;
const testTableName = 'testkv';

class PrismaTestClass {
  @DBOS.transaction()
  static async testTxn(id: string, value: string) {
    const res = await (DBOS.prismaClient as PrismaClient).testkv.create({
      data: {
        id: id,
        value: value,
      },
    });
    globalCnt += 1;
    return res.id;
  }

  @DBOS.transaction({ readOnly: true })
  static async readTxn(id: string) {
    await sleepms(1);
    globalCnt += 1;
    return id;
  }

  @DBOS.transaction()
  static async conflictTxn(id: string, value: string) {
    const res = await (DBOS.prismaClient as PrismaClient).$queryRawUnsafe<testkv>(
      `INSERT INTO ${testTableName} VALUES ($1, $2)`,
      id,
      value,
    );
    return res.id;
  }
}

describe('prisma-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.PRISMA);
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    await DBOS.launch();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id TEXT PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-prisma', async () => {
    const workUUID = randomUUID();
    await expect(
      (await DBOS.startWorkflow(PrismaTestClass, { workflowID: workUUID }).testTxn('test', 'value')).getResult(),
    ).resolves.toBe('test');
    await expect(
      (await DBOS.startWorkflow(PrismaTestClass, { workflowID: workUUID }).testTxn('test', 'value')).getResult(),
    ).resolves.toBe('test');
  });

  test('prisma-duplicate-transaction', async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workUUID = randomUUID();
    let results = await Promise.allSettled([
      (
        await DBOS.startWorkflow(PrismaTestClass, { workflowID: workUUID }).testTxn('oaootest', 'oaoovalue')
      ).getResult(),
      (
        await DBOS.startWorkflow(PrismaTestClass, { workflowID: workUUID }).testTxn('oaootest', 'oaoovalue')
      ).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe('oaootest');
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe('oaootest');
    expect(globalCnt).toBe(1);

    // Read-only transactions would execute twice.
    globalCnt = 0;
    const readUUID = randomUUID();
    results = await Promise.allSettled([
      (await DBOS.startWorkflow(PrismaTestClass, { workflowID: readUUID }).readTxn('oaootestread')).getResult(),
      (await DBOS.startWorkflow(PrismaTestClass, { workflowID: readUUID }).readTxn('oaootestread')).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe('oaootestread');
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe('oaootestread');
    expect(globalCnt).toBeGreaterThanOrEqual(1);
  });

  test('prisma-keyconflict', async () => {
    // Test if we can get the correct Postgres error code from Prisma.
    // We must use query raw, otherwise, Prisma would convert the error to use its own error code.
    const workflowUUID1 = randomUUID();
    const workflowUUID2 = randomUUID();
    const results = await Promise.allSettled([
      (
        await DBOS.startWorkflow(PrismaTestClass, { workflowID: workflowUUID1 }).conflictTxn('conflictkey', 'test1')
      ).getResult(),
      (
        await DBOS.startWorkflow(PrismaTestClass, { workflowID: workflowUUID2 }).conflictTxn('conflictkey', 'test2')
      ).getResult(),
    ]);
    const errorResult = results.find((result) => result.status === 'rejected');
    const err: PrismaClientKnownRequestError = (errorResult as PromiseRejectedResult)
      .reason as PrismaClientKnownRequestError;
    expect((err as unknown as PrismaPGError).meta.code).toBe('23505');
  });
});

const userTableName = 'dbos_test_user';

@Authentication(PUserManager.authMiddlware)
class PUserManager {
  @DBOS.transaction()
  @DBOS.postApi('/register')
  static async createUser(uname: string) {
    const res = await (DBOS.prismaClient as PrismaClient).dbos_test_user.create({
      data: {
        id: 1234,
        username: uname,
      },
    });
    return res;
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
    const u = await ctx.query((dbClient: PrismaClient, uname: string) => {
      return dbClient.dbos_test_user.findFirst({
        where: {
          username: uname,
        },
      });
    }, user as string);

    if (!u) {
      throw new DBOSNotAuthorizedError('User does not exist', 403);
    }
    ctx.logger.info(`Allowed in user: ${u.username}`);
    return {
      authenticatedUser: u.username,
      authenticatedRoles: ['user'],
    };
  }
}

describe('prisma-auth-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.PRISMA);
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

  test('auth-prisma', async () => {
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
  static connectionString?: string;

  @DBOS.transaction()
  static async testEngine() {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
    expect((DBOS.prismaClient as any)._engineConfig.overrideDatasources.db.url).toBe(TestEngine.connectionString);
    const r = await DBOS.prismaClient.$queryRawUnsafe('SELECT 1');
    expect(r.length).toBe(1);
    await Promise.resolve();
  }
}

describe('prisma-engine-config-tests', () => {
  let config: DBOSConfig;

  test('prisma-engine-config', async () => {
    config = {
      name: 'dbostest',
      userDatabaseClient: UserDatabaseName.PRISMA,
      enableUserDatabase: true,
      userDatabasePoolSize: 2, // This is ignored with Prisma
      databaseUrl: `postgresql://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/dbostest?connection_limit=2&connect_timeout=3`,
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
