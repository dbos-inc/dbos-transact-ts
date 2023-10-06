// import { PrismaClient, testkv } from "@prisma/client";
import { EntityManager} from "typeorm";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { Operon, OrmEntities, TransactionContext } from "../src";
import { OperonConfig } from "../src/operon" ;
import { v1 as uuidv1 } from "uuid";
import { TypeORMEntityManager, UserDatabaseName } from "../src/user_database";
import { sleep } from "../src/utils";
import {  Entity, Column, PrimaryColumn } from "typeorm";

/**
 * Funtions used in tests.
 */
@Entity()
export class KV {
    @PrimaryColumn()
    id: string = "t"

    @Column()
    value: string = "v"
}

let globalCnt = 0;

type TestTransactionContext = TransactionContext<TypeORMEntityManager>;


@OrmEntities([KV])
class KVController {

  static async testTxn(txnCtxt: TestTransactionContext,
    id: string,
    value: string) {
      const p = txnCtxt.client as EntityManager ;
      const kv: KV = new KV();
      kv.id = id;
      kv.value = value;
      const res = await p.save(kv);
      globalCnt += 1;
      return res.id;
    }
}

const readTxn = async (txnCtxt: TestTransactionContext, id: string) => {
  await sleep(1);
  globalCnt += 1;
  return id;
};

describe("typeorm-tests", () => {
  const testTableName = "kv";
  let operon: Operon;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig(undefined, UserDatabaseName.TYPEORM);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    operon = new Operon(config);
    await operon.init(KVController);
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id TEXT NOT NULL PRIMARY KEY, value TEXT);`
    ); 
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("simple-typeorm", async () => {
    const workUUID = uuidv1();
    operon.#registerTransaction(KVController.testTxn);
    await expect(
      operon.transaction(KVController.testTxn, { workflowUUID: workUUID }, "test", "value")
    ).resolves.toBe("test");
    await expect(
      operon.transaction(KVController.testTxn, { workflowUUID: workUUID }, "test", "value")
    ).resolves.toBe("test");
  });


  test("typeorm-duplicate-transaction", async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workUUID = uuidv1();
    operon.#registerTransaction(KVController.testTxn);
    let results = await Promise.allSettled([
      operon.transaction(
        KVController.testTxn,
        { workflowUUID: workUUID },
        "oaootest",
        "oaoovalue"
      ),
      operon.transaction(
        KVController.testTxn,
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
    operon.#registerTransaction(readTxn, { readOnly: true });
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

