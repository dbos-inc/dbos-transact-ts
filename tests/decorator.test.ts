import { PoolClient } from "pg";
import { CommunicatorContext, OperonCommunicator, OperonTestingRuntime, OperonTransaction, OperonWorkflow, TransactionContext, WorkflowContext } from "../src";
import { OperonConfig } from "../src/operon";
import { sleep } from "../src/utils";
import { TestKvTable, generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { v1 as uuidv1 } from "uuid";
import { getInternalTestRuntime } from "../src/testing/testing_runtime";

const testTableName = "operon_test_kv";

type TestTransactionContext = TransactionContext<PoolClient>;

class TestClass {
  static #counter = 0;
  static get counter() {
    return TestClass.#counter;
  }
  @OperonCommunicator()
  static async testCommunicator(commCtxt: CommunicatorContext) {
    expect(commCtxt.getConfig("counter")).toBe(3);
    void commCtxt;
    await sleep(1);
    return TestClass.#counter++;
  }

  @OperonWorkflow()
  static async testCommWorkflow(workflowCtxt: WorkflowContext) {
    expect(workflowCtxt.getConfig("counter")).toBe(3);
    const funcResult = await workflowCtxt.invoke(TestClass).testCommunicator();
    return funcResult ?? -1;
  }

  @OperonTransaction()
  static async testInsertTx(txnCtxt: TestTransactionContext, name: string) {
    expect(txnCtxt.getConfig("counter")).toBe(3);
    const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`, [name]);
    return Number(rows[0].id);
  }

  @OperonTransaction({ readOnly: true })
  static async testReadTx(txnCtxt: TestTransactionContext, id: number) {
    const { rows } = await txnCtxt.client.query<TestKvTable>(`SELECT id FROM ${testTableName} WHERE id=$1`, [id]);
    if (rows.length > 0) {
      return Number(rows[0].id);
    } else {
      // Cannot find, return a negative number.
      return -1;
    }
  }

  @OperonWorkflow()
  static async testTxWorkflow(wfCtxt: WorkflowContext, name: string) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(wfCtxt.getConfig("counter")).toBe(3);
    const funcResult: number = await wfCtxt.invoke(TestClass).testInsertTx(name);
    const checkResult: number = await wfCtxt.invoke(TestClass).testReadTx(funcResult);
    return checkResult;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonWorkflow()
  static async nestedWorkflow(wfCtxt: WorkflowContext, name: string) {
    return wfCtxt.childWorkflow(TestClass.testTxWorkflow, name).then((x) => x.getResult());
  }
}

describe("decorator-tests", () => {
  let username: string;
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    username = config.poolConfig.user || "postgres";
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await getInternalTestRuntime([TestClass], config);

    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-communicator-decorator", async () => {
    const workflowUUID: string = uuidv1();
    const initialCounter = TestClass.counter;

    let result: number = await testRuntime
      .invoke(TestClass, workflowUUID)
      .testCommWorkflow()
      .then((x) => x.getResult());
    expect(result).toBe(initialCounter);
    expect(TestClass.counter).toBe(initialCounter + 1);

    // Test OAOO. Should return the original result.
    result = await testRuntime
      .invoke(TestClass, workflowUUID)
      .testCommWorkflow()
      .then((x) => x.getResult());
    expect(result).toBe(initialCounter);
    expect(TestClass.counter).toBe(initialCounter + 1);
  });

  test("wf-decorator-oaoo", async () => {
    let workflowResult: number;
    const uuidArray: string[] = [];

    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidv1();
      uuidArray.push(workflowUUID);
      workflowResult = await testRuntime
        .invoke(TestClass, workflowUUID)
        .testTxWorkflow(username)
        .then((x) => x.getResult());
      expect(workflowResult).toEqual(i + 1);
    }

    // Rerunning with the same workflow UUID should return the same output.
    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidArray[i];
      const workflowResult: number = await testRuntime
        .invoke(TestClass, workflowUUID)
        .testTxWorkflow(username)
        .then((x) => x.getResult());
      expect(workflowResult).toEqual(i + 1);
    }
  });

  test("nested-workflow-oaoo", async () => {
    const operon = testRuntime.getOperon();
    clearInterval(operon.flushBufferID); // Don't flush the output buffer.

    const workflowUUID = uuidv1();
    await expect(
      testRuntime
        .invoke(TestClass, workflowUUID)
        .nestedWorkflow(username)
        .then((x) => x.getResult())
    ).resolves.toBe(1);
    await expect(
      testRuntime
        .invoke(TestClass, workflowUUID)
        .nestedWorkflow(username)
        .then((x) => x.getResult())
    ).resolves.toBe(1);

    // Retrieve output of the child workflow.
    await operon.flushWorkflowStatusBuffer();
    const retrievedHandle = testRuntime.retrieveWorkflow(workflowUUID + "-0");
    await expect(retrievedHandle.getResult()).resolves.toBe(1);
  });
});
