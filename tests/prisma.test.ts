import { PrismaClient, testkv } from "@prisma/client";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { OperonTransaction, TransactionContext } from "../src";
import { v1 as uuidv1 } from "uuid";
import { sleep } from "../src/utils";
import { PrismaClientKnownRequestError } from "@prisma/client/runtime/library";
import { UserDatabaseName } from "../src/user_database";
import { Operon, OperonConfig } from "../src/operon";

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
  let operon: Operon;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig(undefined, UserDatabaseName.PRISMA);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    operon = new Operon(config);
    await operon.init(PrismaTestClass);
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(`CREATE TABLE IF NOT EXISTS ${testTableName} (id TEXT PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("simple-prisma", async () => {
    const workUUID = uuidv1();
    await expect(operon.transaction(PrismaTestClass.testTxn, { workflowUUID: workUUID }, "test", "value")).resolves.toBe("test");
    await expect(operon.transaction(PrismaTestClass.testTxn, { workflowUUID: workUUID }, "test", "value")).resolves.toBe("test");
  });

  test("prisma-duplicate-transaction", async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workUUID = uuidv1();
    let results = await Promise.allSettled([
      operon.transaction(PrismaTestClass.testTxn, { workflowUUID: workUUID }, "oaootest", "oaoovalue"),
      operon.transaction(PrismaTestClass.testTxn, { workflowUUID: workUUID }, "oaootest", "oaoovalue"),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe("oaootest");
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe("oaootest");
    expect(globalCnt).toBe(1);

    // Read-only transactions would execute twice.
    globalCnt = 0;
    const readUUID = uuidv1();
    results = await Promise.allSettled([
      operon.transaction(PrismaTestClass.readTxn, { workflowUUID: readUUID }, "oaootestread"),
      operon.transaction(PrismaTestClass.readTxn, { workflowUUID: readUUID }, "oaootestread"),
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
      operon.transaction(PrismaTestClass.conflictTxn, { workflowUUID: workflowUUID1 }, "conflictkey", "test1"),
      operon.transaction(PrismaTestClass.conflictTxn, { workflowUUID: workflowUUID2 }, "conflictkey", "test2"),
    ]);
    const errorResult = results.find((result) => result.status === "rejected");
    const err: PrismaClientKnownRequestError = (errorResult as PromiseRejectedResult).reason as PrismaClientKnownRequestError;
    expect((err as unknown as PrismaPGError).meta.code).toBe("23505");
  });
});
