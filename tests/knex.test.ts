import {OperonTransaction, OperonWorkflow, TransactionContext, WorkflowContext } from "../src";
import { Operon, OperonConfig } from "../src/operon";
import { UserDatabaseName } from "../src/user_database";
import { TestKvTable, generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { v1 as uuidv1 } from "uuid";
import { Knex } from "knex";

type KnexTransactionContext = TransactionContext<Knex>;
const testTableName = "operon_test_kv";

let insertCount = 0;

class TestClass {
  @OperonTransaction()
  static async testInsert(txnCtxt: KnexTransactionContext, value: string) {
    insertCount++;
    const result = await txnCtxt.client<TestKvTable>(testTableName).insert({ value: value }).returning("id");
    return result[0].id!;
  }

  @OperonTransaction()
  static async testSelect(txnCtxt: KnexTransactionContext, id: number) {
    const result = await txnCtxt.client<TestKvTable>(testTableName).select("value").where({ id: id });
    return result[0].value!;
  }

  @OperonWorkflow()
  static async testWf(ctxt: WorkflowContext, value: string) {
    const id = await ctxt.invoke(TestClass).testInsert(value);
    const result = await ctxt.invoke(TestClass).testSelect(id);
    return result;
  }

  @OperonTransaction()
  static async returnVoid(_ctxt: KnexTransactionContext) {}

  @OperonTransaction()
  static async unsafeInsert(txnCtxt: KnexTransactionContext, key: number, value: string) {
    insertCount++;
    const result = await txnCtxt.client<TestKvTable>(testTableName).insert({ id: key, value: value }).returning("id");
    return result[0].id!;
  }
}

describe("knex-tests", () => {
  let operon: Operon;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig(undefined, UserDatabaseName.KNEX);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    operon = new Operon(config);
    await operon.init(TestClass);
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    insertCount = 0;
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("simple-knex", async () => {
    const insertResult = await operon.transaction(TestClass.testInsert, {}, "test-one");
    expect(insertResult).toEqual(1);
    const selectResult = await operon.transaction(TestClass.testSelect, {}, 1);
    expect(selectResult).toEqual("test-one");
    const wfResult = await operon.workflow(TestClass.testWf, {}, "test-two").then((x) => x.getResult());
    expect(wfResult).toEqual("test-two");
  });

  test("knex-return-void", async () => {
    await expect(operon.transaction(TestClass.returnVoid, {})).resolves.not.toThrow();
  });

  test("knex-duplicate-workflows", async () => {
    const uuid = uuidv1();
    const results = await Promise.allSettled([
      operon.workflow(TestClass.testWf, { workflowUUID: uuid }, "test-one").then((x) => x.getResult()),
      operon.workflow(TestClass.testWf, { workflowUUID: uuid }, "test-one").then((x) => x.getResult()),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe("test-one");
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe("test-one");
    expect(insertCount).toBe(1);
  });

  test("knex-key-conflict", async () => {
    await operon.transaction(TestClass.unsafeInsert, {}, 1, "test-one");
    try {
      await operon.transaction(TestClass.unsafeInsert, {}, 1, "test-two");
      expect(true).toBe(false); // Fail if no error is thrown.
    } catch (e) {
      expect(operon.userDatabase.isKeyConflictError(e)).toBe(true);
    }
  });
});
