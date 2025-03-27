import request from 'supertest';

import { Entity, Column, PrimaryColumn, PrimaryGeneratedColumn } from 'typeorm';
import { EntityManager, Unique } from 'typeorm';

import { generateDBOSTestConfig, generatePublicDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { OrmEntities, Authentication, MiddlewareContext, DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { v1 as uuidv1 } from 'uuid';
import { UserDatabaseName } from '../src/user_database';
import { DBOSInvalidWorkflowTransitionError, DBOSNotAuthorizedError } from '../src/error';

/**
 * Funtions used in tests.
 */
@Entity()
export class KV {
  @PrimaryColumn()
  id: string = 't';

  @Column()
  value: string = 'v';
}

let globalCnt = 0;

@OrmEntities()
export class NoEntities {}

@OrmEntities([KV])
class KVController {
  @DBOS.transaction()
  static async testTxn(id: string, value: string) {
    const kv: KV = new KV();
    kv.id = id;
    kv.value = value;
    const res = await (DBOS.typeORMClient as EntityManager).save(kv);
    globalCnt += 1;
    return res.id;
  }

  @DBOS.transaction({ readOnly: true })
  static async readTxn(id: string) {
    globalCnt += 1;
    const kvp = await (DBOS.typeORMClient as EntityManager).findOneBy(KV, { id: id });
    return Promise.resolve(kvp?.value || '<Not Found>');
  }

  @DBOS.transaction({ readOnly: true })
  static async nope1Txn() {
    return await DBOS.pgClient.query<{ c: string }>('SELECT * FROM t;', []);
  }
  @DBOS.transaction({ readOnly: true })
  static async nope2Txn() {
    return await DBOS.knexClient.raw<{ c: string }>('SELECT * FROM t;', []);
  }
  @DBOS.transaction({ readOnly: true })
  static async nope3Txn() {
    return await DBOS.drizzleClient.select();
  }
  @DBOS.transaction({ readOnly: true })
  static async nope4Txn() {
    return await DBOS.prismaClient.$queryRawUnsafe('SELECT * FROM t;');
  }
}

describe('typeorm-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.TYPEORM);
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    await DBOS.launch();
    await DBOS.dropUserSchema();
    await DBOS.createUserSchema();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-typeorm', async () => {
    await expect(KVController.testTxn('test', 'value')).resolves.toBe('test');

    await expect(KVController.nope1Txn()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
    await expect(KVController.nope2Txn()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
    await expect(KVController.nope3Txn()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
    await expect(KVController.nope4Txn()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
  });

  test('typeorm-duplicate-transaction', async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workUUID = uuidv1();
    let results = await Promise.allSettled([
      (await DBOS.startWorkflow(KVController, { workflowID: workUUID }).testTxn('oaootest', 'oaoovalue')).getResult(),
      (await DBOS.startWorkflow(KVController, { workflowID: workUUID }).testTxn('oaootest', 'oaoovalue')).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe('oaootest');
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe('oaootest');
    expect(globalCnt).toBe(1);

    // Read-only transactions would execute twice.
    globalCnt = 0;
    const readUUID = uuidv1();
    results = await Promise.allSettled([
      (await DBOS.startWorkflow(KVController, { workflowID: readUUID }).readTxn('oaootest')).getResult(),
      (await DBOS.startWorkflow(KVController, { workflowID: readUUID }).readTxn('oaootest')).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe('oaoovalue');
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe('oaoovalue');
    expect(globalCnt).toBeGreaterThanOrEqual(1);
  });
});

@Entity()
@Unique('onlyone', ['username'])
export class User {
  @PrimaryGeneratedColumn('uuid')
  id: string | undefined = undefined;

  @Column()
  username: string = 'user';
}

@OrmEntities([User])
@Authentication(UserManager.authMiddlware)
class UserManager {
  @DBOS.transaction()
  @DBOS.postApi('/register')
  static async createUser(uname: string) {
    const u: User = new User();
    u.username = uname;
    const res = await (DBOS.typeORMClient as EntityManager).save(u);
    return res;
  }

  @DBOS.getApi('/hello')
  @DBOS.requiredRole(['user'])
  static async hello() {
    return Promise.resolve({ messge: 'hello ' + DBOS.authenticatedUser });
  }

  static async authMiddlware(ctx: MiddlewareContext) {
    const cfg = ctx.getConfig<string>('shouldExist', 'does not exist');
    if (cfg !== 'exists') {
      throw Error('Auth is misconfigured.');
    }
    if (!ctx.requiredRole || !ctx.requiredRole.length) {
      return;
    }
    const { user } = ctx.koaContext.query;
    if (!user) {
      throw new DBOSNotAuthorizedError('User not provided', 401);
    }
    const u = await ctx.query((dbClient: EntityManager, uname: string) => {
      return dbClient.findOneBy(User, { username: uname });
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

describe('typeorm-auth-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.TYPEORM);
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    await DBOS.launch();
    DBOS.setUpHandlerCallback();
    await DBOS.dropUserSchema();
    await DBOS.createUserSchema();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('auth-typeorm', async () => {
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
    const ds = DBOS.typeORMClient;
    expect((ds as any).connection.driver.master.options.connectionTimeoutMillis).toBe(3000);
    expect((ds as any).connection.driver.master.options.max).toBe(2);
  }
}

describe('typeorm-engine-config-tests', () => {
  test('engine-config', async () => {
    const config = generatePublicDBOSTestConfig({
      userDbclient: UserDatabaseName.TYPEORM,
      userDbPoolSize: 2,
    });
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
    await TestEngine.testEngine();
    await DBOS.shutdown();
  });
});
