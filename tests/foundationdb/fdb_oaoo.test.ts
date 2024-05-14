import { WorkflowContext, Workflow, TestingRuntime } from "../../src/";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { DBOSConfig } from "../../src/dbos-executor";
import { TestingRuntimeImpl, createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { v1 as uuidv1 } from "uuid";
import { createInternalTestFDB } from "./fdb_helpers";

describe("foundationdb-oaoo", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    const systemDB = await createInternalTestFDB();
    testRuntime = await createInternalTestRuntime([EventStatusOAOO], config, systemDB);
  });

  afterEach(async () => {
    await testRuntime.destroy();
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
      } catch (e) {
        // Ignore error.
        ctxt.logger.error(e);
      }

      const ires = await invokedHandle.getStatus();
      res += "-" + ires?.status;
      return res;
    }
  }

  test("workflow-getevent-retrieve", async () => {
    // Execute a workflow (w/ getUUID) to get an event and retrieve a workflow that doesn't exist, then invoke the setEvent workflow as a child workflow.
    // If we execute the get workflow without UUID, both getEvent and retrieveWorkflow should return values.
    // But if we run the get workflow again with getUUID, getEvent/retrieveWorkflow should still return null.
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    clearInterval(dbosExec.flushBufferID); // Don't flush the output buffer.

    const getUUID = uuidv1();
    const setUUID = getUUID + "-2";

    await expect(
      testRuntime
        .invokeWorkflow(EventStatusOAOO, getUUID)
        .getEventRetrieveWorkflow(setUUID)
    ).resolves.toBe("valueNull-statusNull-PENDING");
    expect(EventStatusOAOO.wfCnt).toBe(2);
    await expect(testRuntime.getEvent(setUUID, "key1")).resolves.toBe("value1");

    EventStatusOAOO.resolve();

    // Wait for the child workflow to finish.
    const handle = testRuntime.retrieveWorkflow(setUUID);
    await expect(handle.getResult()).rejects.toThrow("Failed workflow");

    // Run without UUID, should get the new result.
    await expect(
      testRuntime
        .invokeWorkflow(EventStatusOAOO)
        .getEventRetrieveWorkflow(setUUID)
    ).resolves.toBe("value1-ERROR-ERROR");

    // Test OAOO for getEvent and getWorkflowStatus.
    await expect(
      testRuntime
        .invokeWorkflow(EventStatusOAOO, getUUID)
        .getEventRetrieveWorkflow(setUUID)
    ).resolves.toBe("valueNull-statusNull-PENDING");
    expect(EventStatusOAOO.wfCnt).toBe(6); // Should re-execute the workflow because we're not flushing the result buffer.
  });
});
