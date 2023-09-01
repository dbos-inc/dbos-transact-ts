import { PrismaClient, testkv } from "@prisma/client";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers.js";
import { Operon, OperonConfig, TransactionContext } from "../src/index.js";
import { v1 as uuidv1 } from "uuid";
import { sleep } from "../src/utils.js";
import { PrismaClientKnownRequestError } from "@prisma/client/runtime/library";

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
const testTxn = async (
  txnCtxt: TransactionContext,
  id: string,
  value: string
) => {
  const p: PrismaClient = txnCtxt.prismaClient as PrismaClient;
  const res = await p.testkv.create({
    data: {
      id: id,
      value: value,
    },
  });
  globalCnt += 1;
  return res.id;
};

const readTxn = async (txnCtxt: TransactionContext, id: string) => {
  await sleep(1);
  globalCnt += 1;
  return id;
};

describe("prisma-tests", () => {
  const testTableName = "testkv";
  let operon: Operon;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    operon = new Operon(config);
    operon.usePrisma(new PrismaClient());
    await operon.init();
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id TEXT PRIMARY KEY, value TEXT);`
    );
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("simple-prisma", async () => {
    const workUUID = uuidv1();
    operon.registerTransaction(testTxn);
    await expect(
      operon.transaction(testTxn, { workflowUUID: workUUID }, "test", "value")
    ).resolves.toBe("test");
    await expect(
      operon.transaction(testTxn, { workflowUUID: workUUID }, "test", "value")
    ).resolves.toBe("test");
  });

  test("prisma-duplicate-transaction", async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workUUID = uuidv1();
    operon.registerTransaction(testTxn);
    let results = await Promise.allSettled([
      operon.transaction(
        testTxn,
        { workflowUUID: workUUID },
        "oaootest",
        "oaoovalue"
      ),
      operon.transaction(
        testTxn,
        { workflowUUID: workUUID },
        "oaootest",
        "oaoovalue"
      ),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe(
      "oaootest"
    );
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe(
      "oaootest"
    );
    expect(globalCnt).toBe(1);

    // Read-only transactions would execute twice.
    globalCnt = 0;
    operon.registerTransaction(readTxn, { readOnly: true });
    const readUUID = uuidv1();
    results = await Promise.allSettled([
      operon.transaction(readTxn, { workflowUUID: readUUID }, "oaootestread"),
      operon.transaction(readTxn, { workflowUUID: readUUID }, "oaootestread"),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe(
      "oaootestread"
    );
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe(
      "oaootestread"
    );
    expect(globalCnt).toBeGreaterThanOrEqual(1);
  });

  test("prisma-keyconflict", async () => {
    // Test if we can get the correct Postgres error code from Prisma.
    // We must use query raw, otherwise, Prisma would convert the error to use its own error code.
    const conflictTxn = async (
      txnCtxt: TransactionContext,
      id: string,
      value: string
    ) => {
      const p: PrismaClient = txnCtxt.prismaClient as PrismaClient;
      const res = await p.$queryRawUnsafe<testkv>(
        `INSERT INTO ${testTableName} VALUES ($1, $2)`,
        id,
        value
      );
      return res.id;
    };
    operon.registerTransaction(conflictTxn);
    const workflowUUID1 = uuidv1();
    const workflowUUID2 = uuidv1();
    const results = await Promise.allSettled([
      operon.transaction(
        conflictTxn,
        { workflowUUID: workflowUUID1 },
        "conflictkey",
        "test1"
      ),
      operon.transaction(
        conflictTxn,
        { workflowUUID: workflowUUID2 },
        "conflictkey",
        "test2"
      ),
    ]);
    const errorResult = results.find((result) => result.status === "rejected");
    const err: PrismaClientKnownRequestError = (
      errorResult as PromiseRejectedResult
    ).reason as PrismaClientKnownRequestError;
    expect((err as unknown as PrismaPGError).meta.code).toBe("23505");
  });
});

