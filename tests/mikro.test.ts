import request from 'supertest';

import { Entity, PrimaryKey, Property } from '@mikro-orm/core';
import { EntityManager } from '@mikro-orm/core';

import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { MikroOrmEntities, DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { randomUUID } from 'node:crypto';
import { UserDatabaseName } from '../src/user_database';
import { DBOSInvalidWorkflowTransitionError, DBOSNotAuthorizedError } from '../src/error';

/**
 * Funtions used in tests.
 */
@Entity()
export class KV {
  @PrimaryKey()
  id: string = 't';

  @Property()
  value: string = 'v';
}

let globalCnt = 0;

@MikroOrmEntities()
export class NoEntities {}

@MikroOrmEntities([KV])
class KVController {
  @DBOS.transaction()
  static async testTxn(id: string, value: string) {
    const kv: KV = new KV();
    kv.id = id;
    kv.value = value;
    const res = await (DBOS.mikroORMClient as EntityManager).persist(kv);
    globalCnt += 1;
    return res.id;
  }

  @DBOS.transaction({ readOnly: true })
  static async readTxn(id: string) {
    globalCnt += 1;
    const kvp = await (DBOS.mikroORMClient as EntityManager).findOne(KV, { id: id });
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

describe('mikroorm-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.MIKROORM);
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    await DBOS.launch();
    /* console.log("before each calling dropUserSchema");
    await DBOS.dropUserSchema();
    console.log("before each calling createUserSchema");
    await DBOS.createUserSchema(); */
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-mikroorm', async () => {
    await expect(KVController.testTxn('test', 'value')).resolves.toBe('test');

    await expect(KVController.nope1Txn()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
    await expect(KVController.nope2Txn()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
    await expect(KVController.nope3Txn()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
    await expect(KVController.nope4Txn()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
  });

  /* test('typeorm-duplicate-transaction', async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workUUID = randomUUID();
    let results = await Promise.allSettled([
      (await DBOS.startWorkflow(KVController, { workflowID: workUUID }).testTxn('oaootest', 'oaoovalue')).getResult(),
      (await DBOS.startWorkflow(KVController, { workflowID: workUUID }).testTxn('oaootest', 'oaoovalue')).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe('oaootest');
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe('oaootest');
    expect(globalCnt).toBe(1);

    // Read-only transactions would execute twice.
    globalCnt = 0;
    const readUUID = randomUUID();
    results = await Promise.allSettled([
      (await DBOS.startWorkflow(KVController, { workflowID: readUUID }).readTxn('oaootest')).getResult(),
      (await DBOS.startWorkflow(KVController, { workflowID: readUUID }).readTxn('oaootest')).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe('oaoovalue');
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe('oaoovalue');
    expect(globalCnt).toBeGreaterThanOrEqual(1);
  }); */
});
