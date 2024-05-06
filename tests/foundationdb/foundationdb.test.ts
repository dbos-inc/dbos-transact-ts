import { TransactionContext, CommunicatorContext, WorkflowContext, StatusString, WorkflowHandle, Transaction, Communicator, Workflow, TestingRuntime } from "../../src/";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { v1 as uuidv1 } from "uuid";
import { DBOSConfig } from "../../src/dbos-executor";
import { PoolClient } from "pg";
import { DBOSError } from "../../src/error";
import { TestingRuntimeImpl, createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { createInternalTestFDB } from "./fdb_helpers";

type PGTransactionContext = TransactionContext<PoolClient>;

describe("foundationdb-dbos", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    const systemDB = await createInternalTestFDB();
    testRuntime = await createInternalTestRuntime([FdbTestClass], config, systemDB);
    FdbTestClass.cnt = 0;
    FdbTestClass.wfCnt = 0;
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("fdb-dbos", async () => {
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
    await expect(testRuntime.invoke(FdbTestClass, workflowUUID).testErrorCommunicator()).rejects.toThrow(new DBOSError("Communicator reached maximum retries.", 1));
    await expect(testRuntime.invoke(FdbTestClass, workflowUUID).testErrorCommunicator()).rejects.toThrow(new DBOSError("Communicator reached maximum retries.", 1));
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

    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    await dbosExec.flushWorkflowBuffers();
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

});

class FdbTestClass {
  static cnt = 0;
  static wfCnt = 0;

  @Transaction()
  static async testFunction(_txnCtxt: PGTransactionContext) {
    FdbTestClass.cnt++;
    return Promise.resolve(5);
  }

  @Transaction()
  static async testErrorFunction(_txnCtxt: PGTransactionContext) {
    if (FdbTestClass.cnt++ === 0) {
      return Promise.reject(new Error("fail"));
    }
  }

  @Communicator()
  static async testCommunicator(_commCtxt: CommunicatorContext) {
    return Promise.resolve(FdbTestClass.cnt++);
  }

  @Communicator({ intervalSeconds: 0, maxAttempts: 4 })
  static async testErrorCommunicator(ctxt: CommunicatorContext) {
    FdbTestClass.cnt++;
    if (FdbTestClass.cnt !== ctxt.maxAttempts) {
      throw new Error("bad number");
    }
    return Promise.resolve("success");
  }

  @Communicator({ retriesAllowed: false })
  static async noRetryComm(_ctxt: CommunicatorContext, id: number) {
    FdbTestClass.cnt++;
    return Promise.resolve(id);
  }

  static innerResolve: () => void;
  static innerPromise = new Promise<void>((r) => {
    FdbTestClass.innerResolve = r;
  });

  static outerResolve: () => void;
  static outerPromise = new Promise<void>((r) => {
    FdbTestClass.outerResolve = r;
  });

  @Transaction()
  static async testStatusFunc(_txnCtxt: PGTransactionContext) {
    FdbTestClass.cnt++;
    return Promise.resolve(3);
  }

  @Workflow()
  static async testStatusWorkflow(ctxt: WorkflowContext) {
    const result = ctxt.invoke(FdbTestClass).testStatusFunc();
    FdbTestClass.outerResolve();
    await FdbTestClass.innerPromise;
    return result;
  }

  @Workflow()
  static async receiveWorkflow(ctxt: WorkflowContext) {
    const message1 = await ctxt.recv<string>();
    const message2 = await ctxt.recv<string>();
    const fail = await ctxt.recv("fail", 0);
    return message1 === "message1" && message2 === "message2" && fail === null;
  }

  @Workflow()
  static async sendWorkflow(ctxt: WorkflowContext, destinationUUID: string) {
    await ctxt.send(destinationUUID, "message1");
    await ctxt.send(destinationUUID, "message2");
  }

  @Workflow()
  static async setEventWorkflow(ctxt: WorkflowContext) {
    await ctxt.setEvent("key1", "value1");
    await ctxt.setEvent("key2", "value2");
    return 0;
  }

  @Workflow()
  static async receiveTopicworkflow(ctxt: WorkflowContext, topic: string, timeout: number) {
    return ctxt.recv<string>(topic, timeout);
  }

  static resolve1: () => void;
  static promise1 = new Promise<void>((resolve) => {
    FdbTestClass.resolve1 = resolve;
  });
}
