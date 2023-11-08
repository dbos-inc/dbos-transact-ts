import { WorkflowContext, OperonWorkflow, OperonTestingRuntime } from "../src/";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { OperonConfig } from "../src/operon";
import { OperonTestingRuntimeImpl, createInternalTestRuntime } from "../src/testing/testing_runtime";

describe("recovery-tests", () => {
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime([LocalRecovery, ExecutorRecovery], config);
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

    static cnt = 0;

    @OperonWorkflow()
    static async testRecoveryWorkflow(ctxt: WorkflowContext, input: number) {
      if (ctxt.authenticatedUser === "test_recovery_user" && ctxt.request.url === "test-recovery-url") {
        LocalRecovery.cnt += input;
      }
      await LocalRecovery.promise1;
      return ctxt.authenticatedUser;
    }
  }

  test("local-recovery", async () => {
    // Run a workflow until pending and start recovery.
    const operon = (testRuntime as OperonTestingRuntimeImpl).getOperon();
    clearInterval(operon.flushBufferID); // Don't flush the output buffer, making sure the workflow is executed.

    const handle = await testRuntime.invoke(LocalRecovery, undefined, { authenticatedUser: "test_recovery_user", request: { url: "test-recovery-url" } }).testRecoveryWorkflow(5);

    const recoverHandles = await operon.recoverPendingWorkflows();
    LocalRecovery.resolve1();

    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toBe("test_recovery_user");
    await expect(handle.getResult()).resolves.toBe("test_recovery_user");
    expect(LocalRecovery.cnt).toBe(10); // Should run twice.
  });

  /**
   * Test for selectively recovering workflows run by an executor.
   */
  class ExecutorRecovery {
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

    @OperonWorkflow()
    static async localWorkflow(ctxt: WorkflowContext, input: number) {
      ExecutorRecovery.localCnt += input;
      await ExecutorRecovery.promise1;
      return ctxt.authenticatedUser;
    }

    @OperonWorkflow()
    static async executorWorkflow(ctxt: WorkflowContext, input: number) {
      ExecutorRecovery.executorCnt += input;
      await ExecutorRecovery.promise2;
      return ctxt.authenticatedUser;
    }
  }

  test("selective-recovery", async () => {
    // Invoke a workflow multiple times with different executor IDs, but only recover workflows for a specific executor.
    const operon = (testRuntime as OperonTestingRuntimeImpl).getOperon();
    clearInterval(operon.flushBufferID); // Don't flush the output buffer, making sure workflows are executed.

    const localHandle = await testRuntime.invoke(ExecutorRecovery, undefined, { authenticatedUser: "local_user" }).localWorkflow(3);

    const execHandle = await testRuntime.invoke(ExecutorRecovery, undefined, { authenticatedUser: "cloud_user", request: { headers: { "operon-executorid": "fcvm123" } } }).executorWorkflow(5);

    const recoverHandles = await operon.recoverPendingWorkflows(["fcvm123"]);
    ExecutorRecovery.resolve1();
    ExecutorRecovery.resolve2();

    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toBe("cloud_user");
    await expect(localHandle.getResult()).resolves.toBe("local_user");
    await expect(execHandle.getResult()).resolves.toBe("cloud_user");

    expect(ExecutorRecovery.localCnt).toBe(3); // Should run only once.
    expect(ExecutorRecovery.executorCnt).toBe(10); // Should run twice.
  });
});
