
import {
  TestingRuntime, Transaction,
  TransactionContext,
  Workflow,
  WorkflowContext
  ,
} from "../src";
import { DBOSConfig } from "../src/dbos-executor";
import { UserDatabaseName } from "../src/user_database";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";
import { pgTable, serial, text } from 'drizzle-orm/pg-core';
import { NodePgDatabase } from "drizzle-orm/node-postgres";
import { eq } from "drizzle-orm";
import { v1 as uuidv1 } from "uuid";
import { DatabaseError } from "pg";

const testTableName = "dbos_test_kv";

const testTable = pgTable(testTableName, {
  id: serial('id').primaryKey(),
  value: text('value')
});


let insertCount = 0;

class TestClass {
  @Transaction()
  static async testInsert(txnCtxt: TransactionContext<NodePgDatabase>, value: string) {
    insertCount++;
    const result = await txnCtxt.client.insert(testTable).values({ value }).returning({ id: testTable.id })
    return result[0].id;
  }

  @Transaction()
  static async testSelect(txnCtxt: TransactionContext<NodePgDatabase>, id: number) {
    const result = await txnCtxt.client.select().from(testTable).where(eq(testTable.id, id));
    return result[0].value;
  }

  @Workflow()
  static async testWf(ctxt: WorkflowContext, value: string) {
    const id = await ctxt.invoke(TestClass).testInsert(value);
    const result = await ctxt.invoke(TestClass).testSelect(id);
    return result;
  }

  @Transaction()
  static async returnVoid(_ctxt: TransactionContext<NodePgDatabase>) { }

  @Transaction()
  static async unsafeInsert(txnCtxt: TransactionContext<NodePgDatabase>, key: number, value: string) {
    insertCount++;
    const result = await txnCtxt.client.insert(testTable).values({ id: key, value }).returning({ id: testTable.id })
    return result[0].id;
  }
}

describe("drizzle-tests", () => {
  let testRuntime: TestingRuntime;
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig(UserDatabaseName.DRIZZLE);
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime(undefined, config);
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    insertCount = 0;
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-drizzle", async () => {
    await expect(testRuntime.invoke(TestClass).testInsert("test-one")).resolves.toBe(1);
  });

  test("drizzle-return-void", async () => {
    await expect(testRuntime.invoke(TestClass).returnVoid()).resolves.not.toThrow();
  });

  test("drizzle-duplicate-workflows", async () => {
    const uuid = uuidv1();
    const results = await Promise.allSettled([
      testRuntime.invokeWorkflow(TestClass, uuid).testWf("test-one"),
      testRuntime.invokeWorkflow(TestClass, uuid).testWf("test-one"),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe("test-one");
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe("test-one");
    expect(insertCount).toBe(1);
  });

  test("drizzle-key-conflict", async () => {
    await testRuntime.invoke(TestClass).unsafeInsert(1, "test-one");
    try {
      await testRuntime.invoke(TestClass).unsafeInsert(1, "test-two");
      expect(true).toBe(false); // Fail if no error is thrown.
    } catch (e) {
      const err: DatabaseError = e as DatabaseError;
      expect(err.code).toBe("23505");
    }
  });
});
