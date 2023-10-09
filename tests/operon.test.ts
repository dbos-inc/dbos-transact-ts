import { Operon, WorkflowContext, TransactionContext, CommunicatorContext, WorkflowHandle, OperonTransaction, OperonWorkflow, OperonCommunicator } from "../src/";
import { generateOperonTestConfig, setupOperonTestDb, TestKvTable } from "./helpers";
import { v1 as uuidv1 } from "uuid";
import { StatusString } from "../src/workflow";
import { OperonConfig } from "../src/operon";
import { PoolClient } from "pg";
import { OperonTestingRuntime, OperonTestingRuntimeImpl, createTestingRuntime } from "../src/testing/testing_runtime";

type TestTransactionContext = TransactionContext<PoolClient>;
const testTableName = "operon_test_kv";

describe("operon-tests", () => {
  let username: string;
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;
  let operon: Operon;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    username = config.poolConfig.user || "postgres";
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createTestingRuntime([OperonTestClass], config);
    operon = (testRuntime as OperonTestingRuntimeImpl).getOperon()!;
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    OperonTestClass.cnt = 0;
    OperonTestClass.wfCnt = 0;
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-function", async () => {
    const workflowHandle: WorkflowHandle<string> = await testRuntime.invoke(OperonTestClass).testWorkflow(username);

    expect(typeof workflowHandle.getWorkflowUUID()).toBe("string");
    await expect(workflowHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
      workflowName: OperonTestClass.testWorkflow.name,
    });
    const workflowResult: string = await workflowHandle.getResult();
    expect(JSON.parse(workflowResult)).toEqual({ current_user: username });

    await operon.flushWorkflowStatusBuffer();
    await expect(workflowHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
    const retrievedHandle = testRuntime.retrieveWorkflow<string>(workflowHandle.getWorkflowUUID());
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
    expect(JSON.parse(await retrievedHandle.getResult())).toEqual({
      current_user: username,
    });
  });

  test("return-void", async () => {
    const workflowUUID = uuidv1();
    await testRuntime.invoke(OperonTestClass, workflowUUID).testVoidFunction();
    await expect(testRuntime.invoke(OperonTestClass, workflowUUID).testVoidFunction()).resolves.toBeFalsy();
    await expect(testRuntime.invoke(OperonTestClass, workflowUUID).testVoidFunction()).resolves.toBeFalsy();
  });

  test("tight-loop", async () => {
    for (let i = 0; i < 100; i++) {
      await expect(testRuntime.invoke(OperonTestClass).testNameWorkflow(username).then((x) => x.getResult())).resolves.toBe(username);
    }
  });

  test("abort-function", async () => {
    for (let i = 0; i < 10; i++) {
      await expect(testRuntime.invoke(OperonTestClass).testFailWorkflow(username).then((x) => x.getResult())).resolves.toBe(i + 1);
    }

    // Should not appear in the database.
    await expect(testRuntime.invoke(OperonTestClass).testFailWorkflow("fail").then((x) => x.getResult())).rejects.toThrow("fail");
  });

  test("oaoo-simple", async () => {
    let workflowResult: number;
    const uuidArray: string[] = [];
    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidv1();
      uuidArray.push(workflowUUID);
      workflowResult = await testRuntime.invoke(OperonTestClass, workflowUUID).testOaooWorkflow(username).then((x) => x.getResult());
      expect(workflowResult).toEqual(i + 1);
    }

    // Rerunning with the same workflow UUID should return the same output.
    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidArray[i];
      const workflowResult: number = await testRuntime.invoke(OperonTestClass, workflowUUID).testOaooWorkflow(username).then((x) => x.getResult());
      expect(workflowResult).toEqual(i + 1);
    }
  });

  test("simple-communicator", async () => {
    const workflowUUID: string = uuidv1();

    await expect(testRuntime.invoke(OperonTestClass, workflowUUID).testCommunicator()).resolves.toBe(0);

    // Test OAOO. Should return the original result.
    await expect(testRuntime.invoke(OperonTestClass, workflowUUID).testCommunicator()).resolves.toBe(0);

    // Should be a new run.
    await expect(testRuntime.invoke(OperonTestClass).testCommunicator()).resolves.toBe(1);
  });

  test("simple-workflow-notifications", async () => {
    const workflowUUID = uuidv1();
    const handle = await testRuntime.invoke(OperonTestClass, workflowUUID).receiveWorkflow();
    await testRuntime.invoke(OperonTestClass).sendWorkflow(handle.getWorkflowUUID()).then((x) => x.getResult());
    expect(await handle.getResult()).toBe(true);
    const retry = await testRuntime.invoke(OperonTestClass, workflowUUID).receiveWorkflow().then((x) => x.getResult());
    expect(retry).toBe(true);
  });

  test("notification-oaoo", async () => {
    const recvWorkflowUUID = uuidv1();
    const idempotencyKey = "test-suffix";

    // Send twice with the same idempotency key.  Only one message should be sent.
    await expect(testRuntime.send(recvWorkflowUUID, 123, "testTopic", idempotencyKey)).resolves.not.toThrow();
    await expect(testRuntime.send(recvWorkflowUUID, 123, "testTopic", idempotencyKey)).resolves.not.toThrow();

    // Receive twice with the same UUID.  Each should get the same result of true.
    await expect(testRuntime.invoke(OperonTestClass, recvWorkflowUUID).receiveOaooWorkflow("testTopic", 1).then((x) => x.getResult())).resolves.toBe(true);
    await expect(testRuntime.invoke(OperonTestClass, recvWorkflowUUID).receiveOaooWorkflow("testTopic", 1).then((x) => x.getResult())).resolves.toBe(true);

    // A receive with a different UUID should return false.
    await expect(testRuntime.invoke(OperonTestClass).receiveOaooWorkflow("testTopic", 0).then((x) => x.getResult())).resolves.toBe(false);
  });

  test("simple-workflow-events", async () => {
    const handle: WorkflowHandle<number> = await testRuntime.invoke(OperonTestClass).setEventWorkflow();
    const workflowUUID = handle.getWorkflowUUID();
    await expect(testRuntime.getEvent(workflowUUID, "key1")).resolves.toBe("value1");
    await expect(testRuntime.getEvent(workflowUUID, "key2")).resolves.toBe("value2");
    await expect(testRuntime.getEvent(workflowUUID, "fail", 0)).resolves.toBe(null);
    await handle.getResult();
    await expect(testRuntime.invoke(OperonTestClass, workflowUUID).setEventWorkflow().then((x) => x.getResult())).resolves.toBe(0);
  });

  test("readonly-recording", async () => {
    const workflowUUID = uuidv1();
    // Invoke the workflow, should get the error.
    await expect(testRuntime.invoke(OperonTestClass, workflowUUID).testRecordingWorkflow(123, "test").then((x) => x.getResult())).rejects.toThrowError(new Error("dumb test error"));
    expect(OperonTestClass.cnt).toBe(1);
    expect(OperonTestClass.wfCnt).toBe(2);

    // Invoke it again, should return the recorded same error without running it.
    await expect(testRuntime.invoke(OperonTestClass, workflowUUID).testRecordingWorkflow(123, "test").then((x) => x.getResult())).rejects.toThrowError(new Error("dumb test error"));
    expect(OperonTestClass.cnt).toBe(1);
    expect(OperonTestClass.wfCnt).toBe(2);
  });

  test("retrieve-workflowstatus", async () => {
    const workflowUUID = uuidv1();

    const workflowHandle = await testRuntime.invoke(OperonTestClass, workflowUUID).testStatusWorkflow(123, "hello");

    expect(workflowHandle.getWorkflowUUID()).toBe(workflowUUID);
    await expect(workflowHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
      workflowName: OperonTestClass.testStatusWorkflow.name,
    });

    OperonTestClass.resolve1();
    await OperonTestClass.promise3;

    // Retrieve handle, should get the pending status.
    await expect(testRuntime.retrieveWorkflow<string>(workflowUUID).getStatus()).resolves.toMatchObject({ status: StatusString.PENDING, workflowName: OperonTestClass.testStatusWorkflow.name });

    // Proceed to the end.
    OperonTestClass.resolve2();
    await expect(workflowHandle.getResult()).resolves.toBe("hello");

    // Flush workflow output buffer so the retrieved handle can proceed and the status would transition to SUCCESS.
    await operon.flushWorkflowStatusBuffer();
    const retrievedHandle = testRuntime.retrieveWorkflow<string>(workflowUUID);
    expect(retrievedHandle).not.toBeNull();
    expect(retrievedHandle.getWorkflowUUID()).toBe(workflowUUID);
    await expect(retrievedHandle.getResult()).resolves.toBe("hello");
    await expect(workflowHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });
});

class OperonTestClass {
  static cnt = 0;
  static wfCnt = 0;
  static resolve: () => void;
  static promise = new Promise<void>((r) => {
    OperonTestClass.resolve = r;
  });

  @OperonTransaction()
  static async testFunction(txnCtxt: TestTransactionContext, name: string) {
    const { rows } = await txnCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
    return JSON.stringify(rows[0]);
  }

  @OperonWorkflow()
  static async testWorkflow(ctxt: WorkflowContext, name: string) {
    const funcResult = await ctxt.invoke(OperonTestClass).testFunction(name);
    return funcResult;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction()
  static async testVoidFunction(_txnCtxt: TestTransactionContext) {
    return;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction()
  static async testNameFunction(_txnCtxt: TestTransactionContext, name: string) {
    return name;
  }

  @OperonWorkflow()
  static async testNameWorkflow(ctxt: WorkflowContext, name: string) {
    return ctxt.invoke(OperonTestClass).testNameFunction(name);
  }

  @OperonTransaction()
  static async testFailFunction(txnCtxt: TestTransactionContext, name: string) {
    const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`, [name]);
    if (name === "fail") {
      throw new Error("fail");
    }
    return Number(rows[0].id);
  }

  @OperonTransaction({ readOnly: true })
  static async testKvFunctionRead(txnCtxt: TestTransactionContext, id: number) {
    const { rows } = await txnCtxt.client.query<TestKvTable>(`SELECT id FROM ${testTableName} WHERE id=$1`, [id]);
    if (rows.length > 0) {
      return Number(rows[0].id);
    } else {
      // Cannot find, return a negative number.
      return -1;
    }
  }

  @OperonWorkflow()
  static async testFailWorkflow(workflowCtxt: WorkflowContext, name: string) {
    const funcResult: number = await workflowCtxt.invoke(OperonTestClass).testFailFunction(name);
    const checkResult: number = await workflowCtxt.invoke(OperonTestClass).testKvFunctionRead(funcResult);
    return checkResult;
  }

  @OperonTransaction()
  static async testOaooFunction(txnCtxt: TestTransactionContext, name: string) {
    const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`, [name]);
    return Number(rows[0].id);
  }

  @OperonWorkflow()
  static async testOaooWorkflow(workflowCtxt: WorkflowContext, name: string) {
    const funcResult: number = await workflowCtxt.invoke(OperonTestClass).testOaooFunction(name);
    const checkResult: number = await workflowCtxt.invoke(OperonTestClass).testKvFunctionRead(funcResult);
    return checkResult;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonCommunicator()
  static async testCommunicator(_ctxt: CommunicatorContext) {
    return OperonTestClass.cnt++;
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
  static async receiveOaooWorkflow(ctxt: WorkflowContext, topic: string, timeout: number) {
    // This returns true if and only if exactly one message is sent to it.
    const succeeds = await ctxt.recv<number>(topic, timeout);
    const fails = await ctxt.recv<number>(topic, 0);
    return succeeds === 123 && fails === null;
  }

  @OperonWorkflow()
  static async setEventWorkflow(ctxt: WorkflowContext) {
    await ctxt.setEvent("key1", "value1");
    await ctxt.setEvent("key2", "value2");
    return 0;
  }

  @OperonTransaction({ readOnly: true })
  static async testReadFunction(txnCtxt: TestTransactionContext, id: number) {
    const { rows } = await txnCtxt.client.query<TestKvTable>(`SELECT value FROM ${testTableName} WHERE id=$1`, [id]);
    OperonTestClass.cnt++;
    if (rows.length === 0) {
      return null;
    }
    return rows[0].value;
  }

  @OperonTransaction()
  static async testWriteFunction(txnCtxt: TestTransactionContext, id: number, name: string) {
    const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName} (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value=EXCLUDED.value RETURNING value;`, [
      id,
      name,
    ]);
    return rows[0].value;
  }

  @OperonWorkflow()
  static async testRecordingWorkflow(workflowCtxt: WorkflowContext, id: number, name: string) {
    await workflowCtxt.invoke(OperonTestClass).testReadFunction(id);
    OperonTestClass.wfCnt++;
    await workflowCtxt.invoke(OperonTestClass).testWriteFunction(id, name);
    OperonTestClass.wfCnt++;
    // Make sure the workflow actually runs.
    throw Error("dumb test error");
  }

  // Test workflow status changes correctly.
  static resolve1: () => void;
  static promise1 = new Promise<void>((resolve) => {
    OperonTestClass.resolve1 = resolve;
  });

  static resolve2: () => void;
  static promise2 = new Promise<void>((resolve) => {
    OperonTestClass.resolve2 = resolve;
  });

  static resolve3: () => void;
  static promise3 = new Promise<void>((resolve) => {
    OperonTestClass.resolve3 = resolve;
  });

  @OperonWorkflow()
  static async testStatusWorkflow(workflowCtxt: WorkflowContext, id: number, name: string) {
    await OperonTestClass.promise1;
    const value = await workflowCtxt.invoke(OperonTestClass).testWriteFunction(id, name);
    OperonTestClass.resolve3(); // Signal the execution has done.
    await OperonTestClass.promise2;
    return value;
  }
}
