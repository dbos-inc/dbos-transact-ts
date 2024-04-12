import { TransactionContext, Transaction, OrmEntities } from "../../src/";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { v1 as uuidv1 } from "uuid";
import { DBOSConfig } from "../../src/dbos-executor";
import { TestingRuntime, TestingRuntimeImpl, createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { Column, Entity, EntityManager, PrimaryColumn } from "typeorm";
import { UserDatabaseName } from "../../src/user_database";

type TestTransactionContext = TransactionContext<EntityManager>;

describe("typeorm-debugger-test", () => {
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;
  let testRuntime: TestingRuntime;
  let debugRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.TYPEORM);
    debugConfig = generateDBOSTestConfig(UserDatabaseName.TYPEORM, true);
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    // TODO: connect to the real proxy.
    debugRuntime = await createInternalTestRuntime([KVController], debugConfig);
    testRuntime = await createInternalTestRuntime([KVController], config);
    await testRuntime.dropUserSchema();
    await testRuntime.createUserSchema();
  });

  afterEach(async () => {
    await debugRuntime.destroy();
    await testRuntime.destroy();
  });

  // Test TypeORM
  @Entity()
  class KV {
    @PrimaryColumn()
    id: string = "t";

    @Column()
    value: string = "v";
  }

  @OrmEntities([KV])
  class KVController {
    @Transaction()
    static async testTxn(txnCtxt: TestTransactionContext, id: string, value: string) {
      const kv: KV = new KV();
      kv.id = id;
      kv.value = value;
      const res = await txnCtxt.client.save(kv);
      return res.id;
    }
  }

  test("debug-typeorm-transaction", async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    await expect(testRuntime.invoke(KVController, wfUUID).testTxn("test", "value")).resolves.toBe("test");
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invoke(KVController, wfUUID).testTxn("test", "value")).resolves.toBe("test");

    // Execute again with the provided UUID.
    await expect((debugRuntime as TestingRuntimeImpl).getDBOSExec().executeWorkflowUUID(wfUUID).then((x) => x.getResult())).resolves.toBe("test");
  });
});
