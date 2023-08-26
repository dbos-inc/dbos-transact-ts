// import { PrismaClient, testkv } from "@prisma/client";
import { DataSource, EntityManager } from "typeorm";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { Operon, OperonConfig, TransactionContext } from "src";
import { v1 as uuidv1 } from "uuid";
import { sleep } from "src/utils";


/**
 * Funtions used in tests.
 */
let globalCnt = 0;

const typeormDs = new DataSource({
  type: "postgres", // perhaps should move to config file
  host: "localhost",
  port: 5432,
  username: "postgres",
  password: process.env.PGPASSWORD,
  database: "operontest",
});

/*
interface KV {
  id: string;
} */


const testTxn = async (
  txnCtxt: TransactionContext,
  id: string,
  value: string
) => {
  const p: EntityManager = txnCtxt.typeormEM ;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
  const  res = await p.query("insert into testkv values($1,$2) returning id;",[id, value])
  // console.log(res);
  globalCnt += 1;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-member-access
  return res[0].id;

};

const readTxn = async (txnCtxt: TransactionContext, id: string) => {
  await sleep(1);
  globalCnt += 1;
  return id;
};

describe("typeorm-tests", () => {
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
    operon.useTypeOrm(typeormDs);
    await operon.init();
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id TEXT PRIMARY KEY, value TEXT);`
    );
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("simple-typeorm", async () => {
    const workUUID = uuidv1();
    operon.registerTransaction(testTxn);
    await expect(
      operon.transaction(testTxn, { workflowUUID: workUUID }, "test", "value")
    ).resolves.toBe("test");
    await expect(
      operon.transaction(testTxn, { workflowUUID: workUUID }, "test", "value")
    ).resolves.toBe("test");
  });


  test("typeorm-duplicate-transaction", async () => {
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

});

