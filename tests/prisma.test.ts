import { PrismaClient, testkv } from "@prisma/client";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { OperonTestingRuntime, OperonTransaction, TransactionContext } from "../src";
import { v1 as uuidv1 } from "uuid";
import { sleep } from "../src/utils";
import { PrismaClientKnownRequestError } from "@prisma/client/runtime/library";
import { UserDatabaseName } from "../src/user_database";
import { OperonConfig } from "../src/operon";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";

interface PrismaPGError {
  code: string;
  meta: {
    code: string;
    message: string;
  };
}

type TestTransactionContext = TransactionContext<PrismaClient>;

/**
 * Funtions used in tests.
 */
let globalCnt = 0;
const testTableName = "testkv";

class PrismaTestClass {
  @OperonTransaction()
  static async testTxn(txnCtxt: TestTransactionContext, id: string, value: string) {
    const res = await txnCtxt.client.testkv.create({
      data: {
        id: id,
        value: value,
      },
    });
    globalCnt += 1;
    return res.id;
  }

  @OperonTransaction({ readOnly: true })
  static async readTxn(txnCtxt: TestTransactionContext, id: string) {
    await sleep(1);
    globalCnt += 1;
    return id;
  }

  @OperonTransaction()
  static async conflictTxn(txnCtxt: TestTransactionContext, id: string, value: string) {
    const res = await txnCtxt.client.$queryRawUnsafe<testkv>(`INSERT INTO ${testTableName} VALUES ($1, $2)`, id, value);
    return res.id;
  }
}

describe("prisma-tests", () => {
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig(UserDatabaseName.PRISMA);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    testRuntime = await createInternalTestRuntime([PrismaTestClass], config);
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id TEXT PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-prisma", async () => {
    const workUUID = uuidv1();
    await expect(testRuntime.invoke(PrismaTestClass, workUUID).testTxn("test", "value")).resolves.toBe("test");
    await expect(testRuntime.invoke(PrismaTestClass, workUUID).testTxn("test", "value")).resolves.toBe("test");
  });

  test("prisma-duplicate-transaction", async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workUUID = uuidv1();
    let results = await Promise.allSettled([
      testRuntime.invoke(PrismaTestClass, workUUID).testTxn("oaootest", "oaoovalue"),
      testRuntime.invoke(PrismaTestClass, workUUID).testTxn("oaootest", "oaoovalue"),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe("oaootest");
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe("oaootest");
    expect(globalCnt).toBe(1);

    // Read-only transactions would execute twice.
    globalCnt = 0;
    const readUUID = uuidv1();
    results = await Promise.allSettled([
      testRuntime.invoke(PrismaTestClass, readUUID).readTxn("oaootestread"),
      testRuntime.invoke(PrismaTestClass, readUUID).readTxn("oaootestread"),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe("oaootestread");
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe("oaootestread");
    expect(globalCnt).toBeGreaterThanOrEqual(1);
  });

  test("prisma-keyconflict", async () => {
    // Test if we can get the correct Postgres error code from Prisma.
    // We must use query raw, otherwise, Prisma would convert the error to use its own error code.
    const workflowUUID1 = uuidv1();
    const workflowUUID2 = uuidv1();
    const results = await Promise.allSettled([
      testRuntime.invoke(PrismaTestClass, workflowUUID1).conflictTxn("conflictkey", "test1"),
      testRuntime.invoke(PrismaTestClass, workflowUUID2).conflictTxn("conflictkey", "test2"),
    ]);
    const errorResult = results.find((result) => result.status === "rejected");
    const err: PrismaClientKnownRequestError = (errorResult as PromiseRejectedResult).reason as PrismaClientKnownRequestError;
    expect((err as unknown as PrismaPGError).meta.code).toBe("23505");
  });
});
