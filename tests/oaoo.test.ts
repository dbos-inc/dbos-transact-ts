import { PoolClient } from "pg";
import { CommunicatorContext, Communicator, TestingRuntime, Transaction, Workflow, TransactionContext, WorkflowContext } from "../src";
import { DBOSConfig } from "../src/dbos-executor";
import { TestKvTable, generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { v1 as uuidv1 } from "uuid";
import { TestingRuntimeImpl, createInternalTestRuntime } from "../src/testing/testing_runtime";

const testTableName = "dbos_test_kv";

type TestTransactionContext = TransactionContext<PoolClient>;

describe("oaoo-tests", () => {
  let username: string;
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    username = config.poolConfig.user || "postgres";
    await setUpDBOSTestDb(config);
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
    @Communicator()
    static async testCommunicator(_commCtxt: CommunicatorContext) {
      return Promise.resolve(CommunicatorOAOO.#counter++);
    }

    @Workflow()
    static async testCommWorkflow(workflowCtxt: WorkflowContext) {
      const funcResult = await workflowCtxt.invoke(CommunicatorOAOO).testCommunicator();
      return funcResult ?? -1;
    }
  }

  test("communicator-oaoo", async () => {
    const workflowUUID: string = uuidv1();

    let result: number = await testRuntime
      .invokeWorkflow(CommunicatorOAOO, workflowUUID)
      .testCommWorkflow();
    expect(result).toBe(0);
    expect(CommunicatorOAOO.counter).toBe(1);

    // Test OAOO. Should return the original result.
    result = await testRuntime
      .invokeWorkflow(CommunicatorOAOO, workflowUUID)
      .testCommWorkflow();
    expect(result).toBe(0);
    expect(CommunicatorOAOO.counter).toBe(1);

    // Should be a new run.
    await expect(testRuntime.invokeWorkflow(CommunicatorOAOO).testCommWorkflow()).resolves.toBe(1);
    expect(CommunicatorOAOO.counter).toBe(2);
  });

  /**
   * Workflow OAOO tests.
   */
  class WorkflowOAOO {
    @Transaction()
    static async testInsertTx(txnCtxt: TestTransactionContext, name: string) {
      expect(txnCtxt.getConfig<number>("counter")).toBe(3);
      const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`, [name]);
      return Number(rows[0].id);
    }

    @Transaction({ readOnly: true })
    static async testReadTx(txnCtxt: TestTransactionContext, id: number) {
      const { rows } = await txnCtxt.client.query<TestKvTable>(`SELECT id FROM ${testTableName} WHERE id=$1`, [id]);
      if (rows.length > 0) {
        return Number(rows[0].id);
      } else {
        // Cannot find, return a negative number.
        return -1;
      }
    }

    @Workflow()
    static async testTxWorkflow(wfCtxt: WorkflowContext, name: string) {
      expect(wfCtxt.getConfig<number>("counter")).toBe(3);
      const funcResult: number = await wfCtxt.invoke(WorkflowOAOO).testInsertTx(name);
      const checkResult: number = await wfCtxt.invoke(WorkflowOAOO).testReadTx(funcResult);
      return checkResult;
    }

    @Workflow()
    static async nestedWorkflow(wfCtxt: WorkflowContext, name: string) {
      return await wfCtxt.invokeChildWorkflow(WorkflowOAOO.testTxWorkflow, name);
    }

    @Workflow()
    static async sleepWorkflow(wfCtxt: WorkflowContext, durationSec: number) {
      await wfCtxt.sleep(durationSec);
      return;
    }
  }

  test("workflow-sleep-oaoo", async () => {
    const workflowUUID = uuidv1();
    const initTime = Date.now();
    await expect(testRuntime.invokeWorkflow(WorkflowOAOO, workflowUUID).sleepWorkflow(2)).resolves.toBeFalsy();
    expect(Date.now() - initTime).toBeGreaterThanOrEqual(1500);

    // Rerunning should skip the sleep
    const startTime = Date.now();
    await expect(testRuntime.invokeWorkflow(WorkflowOAOO, workflowUUID).sleepWorkflow(2)).resolves.toBeFalsy();
    expect(Date.now() - startTime).toBeLessThanOrEqual(1000);
  });

  test("workflow-oaoo", async () => {
    let workflowResult: number;
    const uuidArray: string[] = [];

    for (let i = 0; i < 10; i++) {
      const workflowHandle = await testRuntime
        .startWorkflow(WorkflowOAOO)
        .testTxWorkflow(username);
      const workflowUUID: string = workflowHandle.getWorkflowUUID()
      uuidArray.push(workflowUUID);
      workflowResult = await workflowHandle.getResult();
      expect(workflowResult).toEqual(i + 1);
    }

    // Rerunning with the same workflow UUID should return the same output.
    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidArray[i];
      const workflowResult: number = await testRuntime
        .invokeWorkflow(WorkflowOAOO, workflowUUID)
        .testTxWorkflow(username);
      expect(workflowResult).toEqual(i + 1);
    }
  });

  test("nested-workflow-oaoo", async () => {
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    clearInterval(dbosExec.flushBufferID); // Don't flush the output buffer.

    const workflowUUID = uuidv1();
    await expect(
      testRuntime
        .invokeWorkflow(WorkflowOAOO, workflowUUID)
        .nestedWorkflow(username)
    ).resolves.toBe(1);
    await expect(
      testRuntime
        .invokeWorkflow(WorkflowOAOO, workflowUUID)
        .nestedWorkflow(username)
    ).resolves.toBe(1);

    // Retrieve output of the child workflow.
    await dbosExec.flushWorkflowBuffers();
    const retrievedHandle = testRuntime.retrieveWorkflow(workflowUUID + "-0");
    await expect(retrievedHandle.getResult()).resolves.toBe(1);
  });

  /**
   * Workflow notification OAOO tests.
   */
  class NotificationOAOO {
    @Workflow()
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

    // Receive twice with the same UUID.  Each should get the same result of true.
    const recvHandle1 = await testRuntime.startWorkflow(NotificationOAOO, recvWorkflowUUID).receiveOaooWorkflow("testTopic", 1);
    const recvHandle2 = await testRuntime.startWorkflow(NotificationOAOO, recvWorkflowUUID).receiveOaooWorkflow("testTopic", 1);

    // Send twice with the same idempotency key.  Only one message should be sent.
    await expect(testRuntime.send(recvWorkflowUUID, 123, "testTopic", idempotencyKey)).resolves.not.toThrow();
    await expect(testRuntime.send(recvWorkflowUUID, 123, "testTopic", idempotencyKey)).resolves.not.toThrow();

    await expect(recvHandle1.getResult()).resolves.toBe(true)
    await expect(recvHandle2.getResult()).resolves.toBe(true)

    // A receive with a different UUID should return false.
    await expect(testRuntime.invokeWorkflow(NotificationOAOO).receiveOaooWorkflow("testTopic", 0)).resolves.toBe(false);
  });

  /**
   * GetEvent/Status OAOO tests.
   */
  class EventStatusOAOO {
    static wfCnt: number = 0;
    static resolve: () => void;
    static promise = new Promise<void>((r) => {
      EventStatusOAOO.resolve = r;
    });

    @Workflow()
    static async setEventWorkflow(ctxt: WorkflowContext) {
      await ctxt.setEvent("key1", "value1");
      await ctxt.setEvent("key2", "value2");
      await EventStatusOAOO.promise;
      throw Error("Failed workflow");
    }

    @Workflow()
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
      const invokedHandle = await ctxt.startChildWorkflow(EventStatusOAOO.setEventWorkflow);
      try {
        if (EventStatusOAOO.wfCnt > 2) {
          await invokedHandle.getResult();
        }
      } catch(e) {
        // Ignore error.
        ctxt.logger.error(e);
      }

      const ires = await invokedHandle.getStatus();
      res += "-" + ires?.status;
      return res;
    }
  }

  test("workflow-getevent-retrieve", async() => {
    // Execute a workflow (w/ getUUID) to get an event and retrieve a workflow that doesn't exist, then invoke the setEvent workflow as a child workflow.
    // If we execute the get workflow without UUID, both getEvent and retrieveWorkflow should return values.
    // But if we run the get workflow again with getUUID, getEvent/retrieveWorkflow should still return null.
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    clearInterval(dbosExec.flushBufferID); // Don't flush the output buffer.

    const getUUID = uuidv1();
    const setUUID = getUUID + "-2";

    await expect(testRuntime.invokeWorkflow(EventStatusOAOO, getUUID).getEventRetrieveWorkflow(setUUID)).resolves.toBe("valueNull-statusNull-PENDING");
    expect(EventStatusOAOO.wfCnt).toBe(2);
    await expect(testRuntime.getEvent(setUUID, "key1")).resolves.toBe("value1");

    EventStatusOAOO.resolve();

    // Wait for the child workflow to finish.
    const handle = testRuntime.retrieveWorkflow(setUUID);
    await expect(handle.getResult()).rejects.toThrow("Failed workflow");

    // Run without UUID, should get the new result.
    await expect(testRuntime.invokeWorkflow(EventStatusOAOO).getEventRetrieveWorkflow(setUUID)).resolves.toBe("value1-ERROR-ERROR");

    // Test OAOO for getEvent and getWorkflowStatus.
    await expect(testRuntime.invokeWorkflow(EventStatusOAOO, getUUID).getEventRetrieveWorkflow(setUUID)).resolves.toBe("valueNull-statusNull-PENDING");
    expect(EventStatusOAOO.wfCnt).toBe(6);  // Should re-execute the workflow because we're not flushing the result buffer.
  });

});
