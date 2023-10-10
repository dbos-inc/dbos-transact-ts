// import { PrismaClient, testkv } from "@prisma/client";
import { EntityManager } from "typeorm";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { OperonTestingRuntime, OperonTransaction, OrmEntities, TransactionContext } from "../src";
import { OperonConfig } from "../src/operon";
import { v1 as uuidv1 } from "uuid";
import { TypeORMEntityManager, UserDatabaseName } from "../src/user_database";
import { Entity, Column, PrimaryColumn } from "typeorm";
import { getInternalTestRuntime } from "../src/testing/testing_runtime";

/**
 * Funtions used in tests.
 */
@Entity()
export class KV {
  @PrimaryColumn()
  id: string = "t";

  @Column()
  value: string = "v";
}

let globalCnt = 0;

type TestTransactionContext = TransactionContext<TypeORMEntityManager>;

@OrmEntities([KV])
class KVController {
  @OperonTransaction()
  static async testTxn(txnCtxt: TestTransactionContext, id: string, value: string) {
    const p = txnCtxt.client as EntityManager;
    const kv: KV = new KV();
    kv.id = id;
    kv.value = value;
    const res = await p.save(kv);
    globalCnt += 1;
    return res.id;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction({ readOnly: true })
  static async readTxn(_txnCtxt: TestTransactionContext, id: string) {
    globalCnt += 1;
    return id;
  }
}

describe("typeorm-tests", () => {
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig(undefined, UserDatabaseName.TYPEORM);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    testRuntime = await getInternalTestRuntime([KVController], config);
    await testRuntime.dropUserSchema();
    await testRuntime.createUserSchema();
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-typeorm", async () => {
    const workUUID = uuidv1();
    await expect(testRuntime.invoke(KVController, workUUID).testTxn("test", "value")).resolves.toBe("test");
    await expect(testRuntime.invoke(KVController, workUUID).testTxn("test", "value")).resolves.toBe("test");
  });

  test("typeorm-duplicate-transaction", async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workUUID = uuidv1();
    let results = await Promise.allSettled([
      testRuntime.invoke(KVController, workUUID).testTxn("oaootest", 
      "oaoovalue"),
      testRuntime.invoke(KVController, workUUID).testTxn("oaootest", "oaoovalue"),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe("oaootest");
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe("oaootest");
    expect(globalCnt).toBe(1);

    // Read-only transactions would execute twice.
    globalCnt = 0;
    const readUUID = uuidv1();
    results = await Promise.allSettled([
      testRuntime.invoke(KVController, readUUID).readTxn("oaootestread"),
      testRuntime.invoke(KVController, readUUID).readTxn("oaootestread"),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe("oaootestread");
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe("oaootestread");
    expect(globalCnt).toBeGreaterThanOrEqual(1);
  });
});
