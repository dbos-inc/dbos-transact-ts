import { TransactionContext, CommunicatorContext, WorkflowContext, StatusString, WorkflowHandle, OperonTransaction, OperonCommunicator, OperonWorkflow, OperonTestingRuntime } from "../../src/";
import { generateOperonTestConfig, setupOperonTestDb } from "../helpers";
import { FoundationDBSystemDatabase } from "../../src/foundationdb/fdb_system_database";
import { v1 as uuidv1 } from "uuid";
import { OperonConfig } from "../../src/operon";
import { PoolClient } from "pg";
import { OperonError } from "../../src/error";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";

type PGTransactionContext = TransactionContext<PoolClient>;

describe("foundationdb-operon", () => {
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    const systemDB: FoundationDBSystemDatabase = new FoundationDBSystemDatabase();
    testRuntime = await createInternalTestRuntime([FdbTestClass], config, systemDB);
  
    // Clean up tables.
    await systemDB.workflowStatusDB.clearRangeStartsWith("");
    await systemDB.operationOutputsDB.clearRangeStartsWith([]);
    await systemDB.notificationsDB.clearRangeStartsWith([]);
    FdbTestClass.cnt = 0;
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("fdb-operon", async () => {
    const uuid = uuidv1();
    await expect(testRuntime.invoke(FdbTestClass, uuid).testFunction()).resolves.toBe(5);
    await expect(testRuntime.invoke(FdbTestClass, uuid).testFunction()).resolves.toBe(5);
    expect(FdbTestClass.cnt).toBe(1);
  });

  test("fdb-error-recording", async () => {
    const uuid = uuidv1();
    await expect(testRuntime.invoke(FdbTestClass, uuid).testErrorFunction()).rejects.toThrow("fail");
    await expect(testRuntime.invoke(FdbTestClass, uuid).testErrorFunction()).rejects.toThrow("fail");
    expect(FdbTestClass.cnt).toBe(1);
  });

  test("fdb-communicator", async () => {
    const workflowUUID: string = uuidv1();

    await expect(testRuntime.invoke(FdbTestClass, workflowUUID).testCommunicator()).resolves.toBe(0);

    // Test OAOO. Should return the original result.
    await expect(testRuntime.invoke(FdbTestClass, workflowUUID).testCommunicator()).resolves.toBe(0);
    expect(FdbTestClass.cnt).toBe(1);
  });

  test("fdb-communicator-error", async () => {
    await expect(testRuntime.invoke(FdbTestClass).testErrorCommunicator()).resolves.toBe("success");

    const workflowUUID: string = uuidv1();
    await expect(testRuntime.invoke(FdbTestClass, workflowUUID).testErrorCommunicator()).rejects.toThrowError(new OperonError("Communicator reached maximum retries.", 1));
    await expect(testRuntime.invoke(FdbTestClass, workflowUUID).testErrorCommunicator()).rejects.toThrowError(new OperonError("Communicator reached maximum retries.", 1));
  });

  test("fdb-workflow-status", async () => {
    const uuid = uuidv1();
    const invokedHandle = testRuntime.invoke(FdbTestClass, uuid).testStatusWorkflow();
    await FdbTestClass.outerPromise;
    const retrievedHandle = testRuntime.retrieveWorkflow(uuid);
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
    });
    FdbTestClass.innerResolve();
    await expect(invokedHandle.then((x) => x.getResult())).resolves.toBe(3);

    const operon = testRuntime.getOperon();
    await operon.flushWorkflowStatusBuffer();
    await expect(retrievedHandle.getResult()).resolves.toBe(3);
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
    expect(FdbTestClass.cnt).toBe(1);
  });

  test("fdb-notifications", async () => {
    const workflowUUID = uuidv1();
    const handle = await testRuntime.invoke(FdbTestClass, workflowUUID).receiveWorkflow();
    await testRuntime.invoke(FdbTestClass).sendWorkflow(handle.getWorkflowUUID()).then((x) => x.getResult());
    expect(await handle.getResult()).toBe(true);
    const retry = await testRuntime.invoke(FdbTestClass, workflowUUID).receiveWorkflow().then((x) => x.getResult());
    expect(retry).toBe(true);
  });

  test("fdb-simple-workflow-events", async () => {
    const handle: WorkflowHandle<number> = await testRuntime.invoke(FdbTestClass).setEventWorkflow();
    const workflowUUID = handle.getWorkflowUUID();
    await expect(testRuntime.getEvent(workflowUUID, "key1")).resolves.toBe("value1");
    await expect(testRuntime.getEvent(workflowUUID, "key2")).resolves.toBe("value2");
    await expect(testRuntime.getEvent(workflowUUID, "fail", 0)).resolves.toBe(null);
    await handle.getResult();
    await expect(testRuntime.invoke(FdbTestClass, workflowUUID).setEventWorkflow().then((x) => x.getResult())).resolves.toBe(0);
  });

  test("fdb-duplicate-communicator", async () => {
    // Run two communicators concurrently with the same UUID; both should succeed.
    // Since we only record the output after the function, it may cause more than once executions.
    const workflowUUID = uuidv1();
    const results = await Promise.allSettled([
      testRuntime.invoke(FdbTestClass, workflowUUID).noRetryComm(11),
      testRuntime.invoke(FdbTestClass, workflowUUID).noRetryComm(11),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(11);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(11);

    expect(FdbTestClass.cnt).toBeGreaterThanOrEqual(1);
  });

  test("fdb-duplicate-notifications", async () => {
    // Run two send/recv concurrently with the same UUID, both should succeed.
    const recvUUID = uuidv1();
    const recvResPromise = Promise.allSettled([
      testRuntime.invoke(FdbTestClass, recvUUID).receiveTopicworkflow("testTopic", 2).then((x) => x.getResult()),
      testRuntime.invoke(FdbTestClass, recvUUID).receiveTopicworkflow("testTopic", 2).then((x) => x.getResult()),
    ]);

    // Send would trigger both to receive, but only one can delete the message.
    await expect(testRuntime.send(recvUUID, "hello", "testTopic")).resolves.not.toThrow();

    const recvRes = await recvResPromise;
    expect((recvRes[0] as PromiseFulfilledResult<string>).value).toBe("hello");
    expect((recvRes[1] as PromiseFulfilledResult<string>).value).toBe("hello");

    // Make sure we retrieve results correctly.
    await expect(testRuntime.retrieveWorkflow(recvUUID).getResult()).resolves.toBe("hello");
  });

  test("fdb-failure-recovery", async () => {
    // Run a workflow until pending and start recovery.
    const operon = testRuntime.getOperon();
    clearInterval(operon.flushBufferID);

    const handle = await testRuntime.invoke(FdbTestClass, undefined, { authenticatedUser: "test_recovery_user", request: { url: "test-recovery-url" } }).testRecoveryWorkflow(5);

    const recoverPromise = operon.recoverPendingWorkflows();
    FdbTestClass.resolve1();

    await recoverPromise;

    await expect(handle.getResult()).resolves.toBe(5);
    expect(FdbTestClass.cnt).toBe(10); // Should run twice.
  });

  test("workflow-retrieve-event", async () => {
    // Start a workflow that sets the event, and then start a second workflow that get the event and then retrieves the workflow handle to get its final result.
    const handle: WorkflowHandle<number> = await testRuntime.invoke(FdbTestClass).setEventWorkflow();
    const workflowUUID1 = handle.getWorkflowUUID();

    const handle2: WorkflowHandle<string> = await testRuntime.invoke(FdbTestClass).getEventRetrieveWorkflow(workflowUUID1);

    await expect(handle2.getResult()).resolves.toBe("value1-value2-0");
    await expect(handle.getResult()).resolves.toBe(0);
  });
});

class FdbTestClass {
  static cnt = 0;

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction()
  static async testFunction(_txnCtxt: PGTransactionContext) {
    FdbTestClass.cnt++;
    return 5;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction()
  static async testErrorFunction(_txnCtxt: PGTransactionContext) {
    if (FdbTestClass.cnt++ === 0) {
      throw new Error("fail");
    }
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonCommunicator()
  static async testCommunicator(_commCtxt: CommunicatorContext) {
    return FdbTestClass.cnt++;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonCommunicator({ intervalSeconds: 0, maxAttempts: 4 })
  static async testErrorCommunicator(ctxt: CommunicatorContext) {
    FdbTestClass.cnt++;
    if (FdbTestClass.cnt !== ctxt.maxAttempts) {
      throw new Error("bad number");
    }
    return "success";
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonCommunicator({ retriesAllowed: false })
  static async noRetryComm(_ctxt: CommunicatorContext, id: number) {
    FdbTestClass.cnt++;
    return id;
  }

  static innerResolve: () => void;
  static innerPromise = new Promise<void>((r) => {
    FdbTestClass.innerResolve = r;
  });

  static outerResolve: () => void;
  static outerPromise = new Promise<void>((r) => {
    FdbTestClass.outerResolve = r;
  });

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction()
  static async testStatusFunc(_txnCtxt: PGTransactionContext) {
    FdbTestClass.cnt++;
    return 3;
  }

  @OperonWorkflow()
  static async testStatusWorkflow(ctxt: WorkflowContext) {
    const result = ctxt.invoke(FdbTestClass).testStatusFunc();
    FdbTestClass.outerResolve();
    await FdbTestClass.innerPromise;
    return result;
  }

  @OperonWorkflow()
  static async receiveWorkflow(ctxt: WorkflowContext) {
    const message1 = await ctxt.recv<string>();
    const message2 = await ctxt.recv<string>();
    const fail = await ctxt.recv("fail", 0);
    return message1 === "message1" && message2 === "message2" && fail === null;
  }

  @OperonWorkflow()
  static async sendWorkflow(ctxt: WorkflowContext, destinationUUID: string) {
    await ctxt.send(destinationUUID, "message1");
    await ctxt.send(destinationUUID, "message2");
  }

  @OperonWorkflow()
  static async setEventWorkflow(ctxt: WorkflowContext) {
    await ctxt.setEvent("key1", "value1");
    await ctxt.setEvent("key2", "value2");
    return 0;
  }

  @OperonWorkflow()
  static async getEventRetrieveWorkflow(ctxt: WorkflowContext, targetUUID: string): Promise<string> {
    const res1: string | null = await ctxt.getEvent(targetUUID, "key1");
    const res2: string | null = await ctxt.getEvent(targetUUID, "key2");
    if (!res1 || !res2) {
      throw new Error("This shouldn't happen.");
    }
    const handle = ctxt.retrieveWorkflow(targetUUID);
    const output = await handle.getResult();
    return res1 + "-" + res2 + "-" + String(output);
  }

  @OperonWorkflow()
  static async receiveTopicworkflow(ctxt: WorkflowContext, topic: string, timeout: number) {
    return ctxt.recv<string>(topic, timeout);
  }

  static resolve1: () => void;
  static promise1 = new Promise<void>((resolve) => {
    FdbTestClass.resolve1 = resolve;
  });

  @OperonWorkflow()
  static async testRecoveryWorkflow(ctxt: WorkflowContext, input: number) {
    if (ctxt.authenticatedUser === "test_recovery_user" && ctxt.request.url === "test-recovery-url") {
      FdbTestClass.cnt += input;
    }
    await FdbTestClass.promise1;
    return input;
  }
}
