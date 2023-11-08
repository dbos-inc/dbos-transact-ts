import { WorkflowContext, OperonWorkflow, OperonTestingRuntime } from "../../src/";
import { generateOperonTestConfig, setupOperonTestDb } from "../helpers";
import { FoundationDBSystemDatabase } from "../../src/foundationdb/fdb_system_database";
import { OperonConfig } from "../../src/operon";
import { OperonTestingRuntimeImpl, createInternalTestRuntime } from "../../src/testing/testing_runtime";

describe("foundationdb-recovery", () => {
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    const systemDB: FoundationDBSystemDatabase = new FoundationDBSystemDatabase();
    testRuntime = await createInternalTestRuntime([FailureRecovery], config, systemDB, true);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  class FailureRecovery {
    static resolve1: () => void;
    static promise1 = new Promise<void>((resolve) => {
      FailureRecovery.resolve1 = resolve;
    });

    static cnt = 0;

    @OperonWorkflow()
    static async testRecoveryWorkflow(ctxt: WorkflowContext, input: number) {
      if (ctxt.authenticatedUser === "test_recovery_user" && ctxt.request.url === "test-recovery-url") {
        FailureRecovery.cnt += input;
      }
      await FailureRecovery.promise1;
      return ctxt.authenticatedUser;
    }
  }

  test("failure-recovery", async () => {
    // Run a workflow until pending and start recovery.
    const operon = (testRuntime as OperonTestingRuntimeImpl).getOperon();
    clearInterval(operon.flushBufferID); // Don't flush the output buffer.

    const handle = await testRuntime.invoke(FailureRecovery, undefined, { authenticatedUser: "test_recovery_user", request: { url: "test-recovery-url" } }).testRecoveryWorkflow(5);

    const recoverHandles = await operon.recoverPendingWorkflows();
    FailureRecovery.resolve1();

    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toBe("test_recovery_user");
    await expect(handle.getResult()).resolves.toBe("test_recovery_user");
    expect(FailureRecovery.cnt).toBe(10); // Should run twice.
  });
});
