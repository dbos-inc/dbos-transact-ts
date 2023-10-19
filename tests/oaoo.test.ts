import { PoolClient } from "pg";
import { CommunicatorContext, OperonCommunicator, OperonTestingRuntime, OperonTransaction, OperonWorkflow, TransactionContext, WorkflowContext } from "../src";
import { OperonConfig } from "../src/operon";
import { TestKvTable, generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { v1 as uuidv1 } from "uuid";
import { OperonTestingRuntimeImpl, createInternalTestRuntime } from "../src/testing/testing_runtime";

const testTableName = "operon_test_kv";

type TestTransactionContext = TransactionContext<PoolClient>;

describe("oaoo-tests", () => {
  let username: string;
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    username = config.poolConfig.user || "postgres";
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime([CommunicatorOAOO, WorkflowOAOO, NotificationOAOO, EventStatusOAOO], config);

    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  /**
   * Communicator OAOO tests.
   */
  class CommunicatorOAOO {
    static #counter = 0;
    static get counter() {
      return CommunicatorOAOO.#counter;
    }
    // eslint-disable-next-line @typescript-eslint/require-await
    @OperonCommunicator()
    static async testCommunicator(_commCtxt: CommunicatorContext) {
      return CommunicatorOAOO.#counter++;
    }
  
    @OperonWorkflow()
    static async testCommWorkflow(workflowCtxt: WorkflowContext) {
      const funcResult = await workflowCtxt.invoke(CommunicatorOAOO).testCommunicator();
      return funcResult ?? -1;
    }
  }

  test("communicator-oaoo", async () => {
    const workflowUUID: string = uuidv1();

    let result: number = await testRuntime
      .invoke(CommunicatorOAOO, workflowUUID)
      .testCommWorkflow()
      .then((x) => x.getResult());
    expect(result).toBe(0);
    expect(CommunicatorOAOO.counter).toBe(1);

    // Test OAOO. Should return the original result.
    result = await testRuntime
      .invoke(CommunicatorOAOO, workflowUUID)
      .testCommWorkflow()
      .then((x) => x.getResult());
    expect(result).toBe(0);
    expect(CommunicatorOAOO.counter).toBe(1);

    // Should be a new run.
    await expect(testRuntime.invoke(CommunicatorOAOO).testCommWorkflow().then(x => x.getResult())).resolves.toBe(1);
    expect(CommunicatorOAOO.counter).toBe(2);
  });

  /**
   * Workflow OAOO tests.
   */
  class WorkflowOAOO {
    @OperonTransaction()
    static async testInsertTx(txnCtxt: TestTransactionContext, name: string) {
      expect(txnCtxt.getConfig<number>("counter")).toBe(3);
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
      expect(wfCtxt.getConfig<number>("counter")).toBe(3);
      const funcResult: number = await wfCtxt.invoke(WorkflowOAOO).testInsertTx(name);
      const checkResult: number = await wfCtxt.invoke(WorkflowOAOO).testReadTx(funcResult);
      return checkResult;
    }
  
    // eslint-disable-next-line @typescript-eslint/require-await
    @OperonWorkflow()
    static async nestedWorkflow(wfCtxt: WorkflowContext, name: string) {
      return wfCtxt.childWorkflow(WorkflowOAOO.testTxWorkflow, name).then((x) => x.getResult());
    }
  }

  test("workflow-oaoo", async () => {
    let workflowResult: number;
    const uuidArray: string[] = [];

    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidv1();
      uuidArray.push(workflowUUID);
      workflowResult = await testRuntime
        .invoke(WorkflowOAOO, workflowUUID)
        .testTxWorkflow(username)
        .then((x) => x.getResult());
      expect(workflowResult).toEqual(i + 1);
    }

    // Rerunning with the same workflow UUID should return the same output.
    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidArray[i];
      const workflowResult: number = await testRuntime
        .invoke(WorkflowOAOO, workflowUUID)
        .testTxWorkflow(username)
        .then((x) => x.getResult());
      expect(workflowResult).toEqual(i + 1);
    }
  });

  test("nested-workflow-oaoo", async () => {
    const operon = (testRuntime as OperonTestingRuntimeImpl).getOperon();
    clearInterval(operon.flushBufferID); // Don't flush the output buffer.

    const workflowUUID = uuidv1();
    await expect(
      testRuntime
        .invoke(WorkflowOAOO, workflowUUID)
        .nestedWorkflow(username)
        .then((x) => x.getResult())
    ).resolves.toBe(1);
    await expect(
      testRuntime
        .invoke(WorkflowOAOO, workflowUUID)
        .nestedWorkflow(username)
        .then((x) => x.getResult())
    ).resolves.toBe(1);

    // Retrieve output of the child workflow.
    await operon.flushWorkflowStatusBuffer();
    const retrievedHandle = testRuntime.retrieveWorkflow(workflowUUID + "-0");
    await expect(retrievedHandle.getResult()).resolves.toBe(1);
  });

  /**
   * Workflow notification OAOO tests.
   */
  class NotificationOAOO {
    @OperonWorkflow()
    static async receiveOaooWorkflow(ctxt: WorkflowContext, topic: string, timeout: number) {
      // This returns true if and only if exactly one message is sent to it.
      const succeeds = await ctxt.recv<number>(topic, timeout);
      const fails = await ctxt.recv<number>(topic, 0);
      return succeeds === 123 && fails === null;
    }
  }

  test("notification-oaoo", async () => {
    const recvWorkflowUUID = uuidv1();
    const idempotencyKey = "test-suffix";

    // Send twice with the same idempotency key.  Only one message should be sent.
    await expect(testRuntime.send(recvWorkflowUUID, 123, "testTopic", idempotencyKey)).resolves.not.toThrow();
    await expect(testRuntime.send(recvWorkflowUUID, 123, "testTopic", idempotencyKey)).resolves.not.toThrow();

    // Receive twice with the same UUID.  Each should get the same result of true.
    await expect(testRuntime.invoke(NotificationOAOO, recvWorkflowUUID).receiveOaooWorkflow("testTopic", 1).then((x) => x.getResult())).resolves.toBe(true);
    await expect(testRuntime.invoke(NotificationOAOO, recvWorkflowUUID).receiveOaooWorkflow("testTopic", 1).then((x) => x.getResult())).resolves.toBe(true);

    // A receive with a different UUID should return false.
    await expect(testRuntime.invoke(NotificationOAOO).receiveOaooWorkflow("testTopic", 0).then((x) => x.getResult())).resolves.toBe(false);
  });

  /**
   * GetEvent/Status OAOO tests.
   */
  class EventStatusOAOO {
    static wfCnt: number = 0;

    @OperonWorkflow()
    static async setEventWorkflow(ctxt: WorkflowContext) {
      await ctxt.setEvent("key1", "value1");
      await ctxt.setEvent("key2", "value2");
      return 0;
    }

    @OperonWorkflow()
    static async getEventRetrieveWorkflow(ctxt: WorkflowContext, targetUUID: string): Promise<string> {
      let res = "";
      const getValue = await ctxt.getEvent<string>(targetUUID, "key1", 0);
      EventStatusOAOO.wfCnt++;
      if (getValue === null) {
        res = "valueNull";
      } else {
        res = getValue;
      }
  
      const handle = ctxt.retrieveWorkflow(targetUUID);
      const status = await handle.getStatus();
      EventStatusOAOO.wfCnt++;
      if (status === null) {
        res += "-statusNull";
      } else {
        res += "-" + status.status;
      }
  
      // Note: the targetUUID must match the child workflow UUID.
      const value = await ctxt.childWorkflow(EventStatusOAOO.setEventWorkflow).then(x => x.getResult());
      res += "-" + value;
      return res;
    }
  }

  test("workflow-getevent-retrieve", async() => {
    // Execute a workflow (w/ getUUID) to get an event and retrieve a workflow that doesn't exist, then invoke the setEvent workflow as a child workflow.
    // If we execute the get workflow without UUID, both getEvent and retrieveWorkflow should return values.
    // But if we run the get workflow again with getUUID, getEvent/retrieveWorkflow should still return null.
    const operon = (testRuntime as OperonTestingRuntimeImpl).getOperon();
    clearInterval(operon.flushBufferID); // Don't flush the output buffer.

    const getUUID = uuidv1();
    const setUUID = getUUID + "-2";

    await expect(testRuntime.invoke(EventStatusOAOO, getUUID).getEventRetrieveWorkflow(setUUID).then(x => x.getResult())).resolves.toBe("valueNull-statusNull-0");
    expect(EventStatusOAOO.wfCnt).toBe(2);
    await expect(testRuntime.getEvent(setUUID, "key1")).resolves.toBe("value1");

    // Run without UUID, should get the new result.
    await expect(testRuntime.invoke(EventStatusOAOO).getEventRetrieveWorkflow(setUUID).then(x => x.getResult())).resolves.toBe("value1-PENDING-0");

    // Test OAOO for getEvent and getWorkflowStatus.
    await expect(testRuntime.invoke(EventStatusOAOO, getUUID).getEventRetrieveWorkflow(setUUID).then(x => x.getResult())).resolves.toBe("valueNull-statusNull-0");
    expect(EventStatusOAOO.wfCnt).toBe(6);  // Should re-execute the workflow because we're not flushing the result buffer.
  });

});
