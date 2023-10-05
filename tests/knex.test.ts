import { Operon, OperonTransaction, OperonWorkflow, TransactionContext, WorkflowContext } from "../src";
import { OperonConfig } from "../src/operon";
import { UserDatabaseName } from "../src/user_database";
import { TestKvTable, generateOperonTestConfig, setupOperonTestDb } from "./helpers";

const testTableName = "operon_test_kv";

class TestClass {
  @OperonTransaction()
  static async testInsert(txnCtxt: TransactionContext, name: string) {
    const result = await txnCtxt.knex<TestKvTable>(testTableName)
      .insert({ value: name })
      .returning('id');
    return result[0].id!;
  }

  @OperonTransaction()
  static async testSelect(txnCtxt: TransactionContext, id: number) {
    const result = await txnCtxt.knex<TestKvTable>(testTableName).select("value").where({id: id});
    return result[0].value!;
  }

  @OperonWorkflow()
  static async testWf(ctxt: WorkflowContext, name: string) {
    const id = await ctxt.invoke(TestClass).testInsert(name);
    const result = await ctxt.invoke(TestClass).testSelect(id);
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
    const insertResult = await operon.transaction(TestClass.testInsert, {}, "test-one");
    expect(insertResult).toEqual(1);
    const selectResult = await operon.transaction(TestClass.testSelect, {}, 1);
    expect(selectResult).toEqual("test-one");
    const wfResult = await operon.workflow(TestClass.testWf, {}, "test-two").then(x => x.getResult());
    expect(wfResult).toEqual("test-two");
  });
});