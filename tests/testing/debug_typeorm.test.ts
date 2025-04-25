import { DBOS, OrmEntities } from '../../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';
import { randomUUID } from 'node:crypto';
import { DBOSConfig, DebugMode } from '../../src/dbos-executor';
import { Column, Entity, EntityManager, PrimaryColumn } from 'typeorm';
import { UserDatabaseName } from '../../src/user_database';

describe('typeorm-debugger-test', () => {
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.TYPEORM);
    debugConfig = generateDBOSTestConfig(UserDatabaseName.TYPEORM);
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    // TODO: connect to the real proxy.
    DBOS.setConfig(config);
    await DBOS.launch();
    await DBOS.dropUserSchema();
    await DBOS.createUserSchema();
    await DBOS.shutdown();
  });

  // Test TypeORM
  @Entity()
  class KV {
    @PrimaryColumn()
    id: string = 't';

    @Column()
    value: string = 'v';
  }

  @OrmEntities([KV])
  class KVController {
    @DBOS.transaction()
    static async testTxn(id: string, value: string) {
      const kv: KV = new KV();
      kv.id = id;
      kv.value = value;
      const res = await (DBOS.typeORMClient as EntityManager).save(kv);
      return res.id;
    }
  }

  test('debug-typeorm-transaction', async () => {
    const wfUUID = randomUUID();
    // Execute the workflow and destroy the runtime
    DBOS.setConfig(config);
    await DBOS.launch();
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(KVController.testTxn('test', 'value')).resolves.toBe('test');
    });
    await DBOS.shutdown();

    // Execute again in debug mode.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: DebugMode.ENABLED });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(KVController.testTxn('test', 'value')).resolves.toBe('test');
    });

    // Execute again with the provided UUID.
    await expect(DBOS.executeWorkflowById(wfUUID).then((x) => x.getResult())).resolves.toBe('test');
    await DBOS.shutdown();
  });
});
