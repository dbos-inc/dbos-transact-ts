import { PoolClient } from "pg";
import { TransactionContext } from "../../src/transaction";
import { TestingRuntime, DBOSTransaction, DBOSWorkflow, WorkflowContext, createTestingRuntime } from "../../src";

type TestTransactionContext = TransactionContext<PoolClient>;

describe("testruntime-test", () => {
  const username = "postgres";
  const configFilePath = "operon-test-config.yaml";
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    testRuntime = await createTestingRuntime([TestClass], configFilePath);
  });

  afterAll(async () => {
    await testRuntime.destroy();
  });

  test("simple-workflow", async () => {
    const res = await testRuntime.invoke(TestClass).testWorkflow(username).then(x => x.getResult());
    const expectName = testRuntime.getConfig<string>("testvalue"); // Read application config.
    expect(JSON.parse(res)).toEqual({ current_user: expectName });
  });

});

class TestClass {
  @DBOSTransaction()
  static async testFunction(txnCtxt: TestTransactionContext, name: string) {
    const { rows } = await txnCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
    txnCtxt.logger.debug("Name: " + name);
    return JSON.stringify(rows[0]);
  }

  @DBOSWorkflow()
  static async testWorkflow(ctxt: WorkflowContext, name: string) {
    const funcResult = await ctxt.invoke(TestClass).testFunction(name);
    return funcResult;
  }
}