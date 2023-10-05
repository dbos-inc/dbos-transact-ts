import { Operon, OperonTransaction, TransactionContext } from "../src";
import { OperonConfig } from "../src/operon";
import { UserDatabaseName } from "../src/user_database";
import { TestKvTable, generateOperonTestConfig, setupOperonTestDb } from "./helpers";

const testTableName = "operon_test_kv";

class TestClass {
  @OperonTransaction()
  static async testTx(txnCtxt: TransactionContext, name: string) {
    const result = await txnCtxt.knex<TestKvTable>(testTableName)
      .insert({ value: name })
      .returning('id');
    return result;
  }
}

describe("knex-tests", () => {

  let operon: Operon;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig(undefined, UserDatabaseName.KNEX);
    await setupOperonTestDb(config);
  })


  beforeEach(async () => {
    operon = new Operon(config);
    await operon.init(TestClass);
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`
    );
  });

  afterEach(async () => {
    await operon.destroy();
  });


  test("simple-knex", async () => {
    const result = await operon.transaction(TestClass.testTx, {}, "bob");
    expect(result[0].id).toEqual(1);
  });
});