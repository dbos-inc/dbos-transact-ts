import { WorkflowContext, Workflow, TestingRuntime } from "../../src/";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { DBOSConfig } from "../../src/dbos-executor";
import { TestingRuntimeImpl, createInternalTestRuntime } from "../../src/testing/testing_runtime";
import request from "supertest";
import { WorkflowRecoveryUrl } from "../../src/httpServer/server";
import { createInternalTestFDB } from "./fdb_helpers";

describe("foundationdb-recovery", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    const systemDB = await createInternalTestFDB();
    testRuntime = await createInternalTestRuntime([LocalRecovery, ExecutorRecovery], config, systemDB);
    process.env.DBOS__VMID = ""
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  /**
   * Test for the default local workflow recovery.
   */
  class LocalRecovery {
    static resolve1: () => void;
    static promise1 = new Promise<void>((resolve) => {
      LocalRecovery.resolve1 = resolve;
    });

    static resolve2: () => void;
    static promise2 = new Promise<void>((resolve) => {
      LocalRecovery.resolve2 = resolve;
    });

    static cnt = 0;

    @Workflow()
    static async testRecoveryWorkflow(ctxt: WorkflowContext, input: number) {
      if (ctxt.authenticatedUser === "test_recovery_user" && ctxt.request.url === "test-recovery-url") {
        LocalRecovery.cnt += input;
      }

      // Signal the workflow has been executed more than once.
      if (LocalRecovery.cnt > input) {
        LocalRecovery.resolve2();
      }

      await LocalRecovery.promise1;
      return ctxt.authenticatedUser;
    }
  }

  test("local-recovery", async () => {
    // Run a workflow until pending and start recovery.
    // This test simulates a "local" environment because the request parameter does not have an dbos-executor-id header.
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const handle = await testRuntime.startWorkflow(LocalRecovery, undefined, { authenticatedUser: "test_recovery_user", request: { url: "test-recovery-url" } }).testRecoveryWorkflow(5);

    const recoverHandles = await dbosExec.recoverPendingWorkflows();
    await LocalRecovery.promise2; // Wait for the recovery to be done.
    LocalRecovery.resolve1(); // Both can finish now.

    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toBe("test_recovery_user");
    await expect(handle.getResult()).resolves.toBe("test_recovery_user");
    expect(LocalRecovery.cnt).toBe(10); // Should run twice.
  });

  /**
   * Test for selectively recovering workflows run by an executor.
   */
  class ExecutorRecovery {
    static localResolve: () => void;
    static localPromise = new Promise<void>((resolve) => {
      ExecutorRecovery.localResolve = resolve;
    });

    static resolve1: () => void;
    static promise1 = new Promise<void>((resolve) => {
      ExecutorRecovery.resolve1 = resolve;
    });

    static resolve2: () => void;
    static promise2 = new Promise<void>((resolve) => {
      ExecutorRecovery.resolve2 = resolve;
    });

    static localCnt = 0;
    static executorCnt = 0;

    @Workflow()
    static async localWorkflow(ctxt: WorkflowContext, input: number) {
      ExecutorRecovery.localCnt += input;
      await ExecutorRecovery.localPromise;
      return ctxt.authenticatedUser;
    }

    @Workflow()
    static async executorWorkflow(ctxt: WorkflowContext, input: number) {
      ExecutorRecovery.executorCnt += input;

      // Signal the workflow has been executed more than once.
      if (ExecutorRecovery.executorCnt > input) {
        ExecutorRecovery.resolve2();
      }

      await ExecutorRecovery.promise1;
      return ctxt.authenticatedUser;
    }
  }

  test("selective-recovery", async () => {
    // Invoke a workflow multiple times with different executor IDs, but only recover workflows for a specific executor.
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const localHandle = await testRuntime.startWorkflow(ExecutorRecovery, undefined, { authenticatedUser: "local_user" }).localWorkflow(3);

    process.env.DBOS__VMID = "fcvm123"
    const execHandle = await testRuntime.startWorkflow(ExecutorRecovery, undefined, { authenticatedUser: "cloud_user" }).executorWorkflow(5);

    const recoverHandles = await dbosExec.recoverPendingWorkflows(["fcvm123"]);
    await ExecutorRecovery.promise2; // Wait for the recovery to be done.
    ExecutorRecovery.resolve1();
    ExecutorRecovery.localResolve();

    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toBe("cloud_user");
    await expect(localHandle.getResult()).resolves.toBe("local_user");
    await expect(execHandle.getResult()).resolves.toBe("cloud_user");

    expect(ExecutorRecovery.localCnt).toBe(3); // Should run only once.
    expect(ExecutorRecovery.executorCnt).toBe(10); // Should run twice.
  });

  test("http-recovery", async () => {
    // Invoke a workflow and invoke a recovery through HTTP endpoint.
    // Reset variables.
    ExecutorRecovery.executorCnt = 0;
    ExecutorRecovery.promise1 = new Promise<void>((resolve) => {
      ExecutorRecovery.resolve1 = resolve;
    });
    ExecutorRecovery.promise2 = new Promise<void>((resolve) => {
      ExecutorRecovery.resolve2 = resolve;
    });

    process.env.DBOS__VMID = "fcvm123"
    const execHandle = await testRuntime.startWorkflow(ExecutorRecovery, undefined, { authenticatedUser: "cloud_user" }).executorWorkflow(5);

    const response = await request(testRuntime.getAdminCallback())
      .post(WorkflowRecoveryUrl)
      .send(["fcvm123"]);
    expect(response.statusCode).toBe(200);
    expect(response.body).toStrictEqual([execHandle.getWorkflowUUID()]);

    await ExecutorRecovery.promise2; // Wait for the recovery to be done.
    ExecutorRecovery.resolve1();

    // Check output.
    await expect(execHandle.getResult()).resolves.toBe("cloud_user");
    expect(ExecutorRecovery.executorCnt).toBe(10); // Should run twice.
  });
});
