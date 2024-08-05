
import {
  TestingRuntime, Transaction,
  TransactionContext
  ,
} from "../src";
import { DBOSConfig } from "../src/dbos-executor";
import { UserDatabaseName } from "../src/user_database";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";
import { pgTable, serial, text } from 'drizzle-orm/pg-core';
import { NodePgDatabase } from "drizzle-orm/node-postgres";


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
    const result = await txnCtxt.client.insert(testTable).values({value}).returning({id: testTable.id})
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
});


