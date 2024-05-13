import { PoolClient } from "pg";
import { TransactionContext } from "../../src/transaction";
import { TestingRuntime, Transaction, Workflow, WorkflowContext, createTestingRuntime } from "../../src";
import { v1 as uuidv1 } from "uuid";

type TestTransactionContext = TransactionContext<PoolClient>;

describe("testruntime-test", () => {
  const username = "postgres";
  const configFilePath = "dbos-test-config.yaml";
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    testRuntime = await createTestingRuntime([TestClass], configFilePath);
  });

  afterAll(async () => {
    await testRuntime.destroy();
  });

  test("simple-workflow", async () => {
    const uuid = uuidv1();
    const res = await testRuntime
      .invokeWorkflow(TestClass, uuid)
      .testWorkflow(username);
    const expectName = testRuntime.getConfig<string>("testvalue"); // Read application config.
    expect(JSON.parse(res)).toEqual({ current_user: expectName });

    // Create a new testing runtime but don't drop the system DB.
    const newTestRuntime = await createTestingRuntime([TestClass], configFilePath, false);
    const handle = newTestRuntime.retrieveWorkflow(uuid);
    expect(JSON.parse((await handle.getResult()) as string)).toEqual({ current_user: expectName });
    await newTestRuntime.destroy();
  });
});

class TestClass {
  @Transaction()
  static async testFunction(txnCtxt: TestTransactionContext, name: string) {
    const { rows } = await txnCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
    txnCtxt.logger.debug("Name: " + name);
    return JSON.stringify(rows[0]);
  }

  @Workflow()
  static async testWorkflow(ctxt: WorkflowContext, name: string) {
    const funcResult = await ctxt.invoke(TestClass).testFunction(name);
    return funcResult;
  }
}
