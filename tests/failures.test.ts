import { WorkflowContext, TransactionContext, CommunicatorContext, OperonCommunicator, OperonWorkflow, OperonTransaction, ArgOptional, OperonTestingRuntime } from "../src/";
import { generateOperonTestConfig, setupOperonTestDb, TestKvTable } from "./helpers";
import { DatabaseError, PoolClient } from "pg";
import { v1 as uuidv1 } from "uuid";
import { StatusString } from "../src/workflow";
import { OperonError } from "../src/error";
import { OperonConfig } from "../src/operon";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";

const testTableName = "operon_failure_test_kv";
type TestTransactionContext = TransactionContext<PoolClient>;

describe("failures-tests", () => {
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime([FailureTestClass], config);
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`);
    FailureTestClass.cnt = 0;
    FailureTestClass.success = "";
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("operon-error", async () => {
    const wfUUID1 = uuidv1();
    await expect(testRuntime.invoke(FailureTestClass, wfUUID1).testCommunicator(11)).rejects.toThrowError(new OperonError("test operon error with code.", 11));

    const retrievedHandle = testRuntime.retrieveWorkflow<string>(wfUUID1);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.ERROR,
    });
    await expect(retrievedHandle.getResult()).rejects.toThrowError(new OperonError("test operon error with code.", 11));

    // Test without code.
    const wfUUID = uuidv1();
    await expect(testRuntime.invoke(FailureTestClass, wfUUID).testCommunicator()).rejects.toThrowError(new OperonError("test operon error without code"));
  });

  test("readonly-error", async () => {
    const testUUID = uuidv1();
    await expect(testRuntime.invoke(FailureTestClass, testUUID).testReadonlyError()).rejects.toThrowError(new Error("test error"));
    expect(FailureTestClass.cnt).toBe(1);

    // The error should be recorded in the database, so the function shouldn't run again.
    await expect(testRuntime.invoke(FailureTestClass, testUUID).testReadonlyError()).rejects.toThrowError(new Error("test error"));
    expect(FailureTestClass.cnt).toBe(1);
  });

  test("simple-keyconflict", async () => {
    const workflowUUID1 = uuidv1();
    const workflowUUID2 = uuidv1();

    // Start two concurrent transactions.
    const results = await Promise.allSettled([
      testRuntime.invoke(FailureTestClass, workflowUUID1).testKeyConflict(10, workflowUUID1),
      testRuntime.invoke(FailureTestClass, workflowUUID2).testKeyConflict(10, workflowUUID2),
    ]);
    const errorResult = results.find((result) => result.status === "rejected");
    const err: DatabaseError = (errorResult as PromiseRejectedResult).reason as DatabaseError;
    expect(err.code).toBe("23505");
    expect(err.table?.toLowerCase()).toBe(testTableName.toLowerCase());

    expect(FailureTestClass.cnt).toBe(1);

    // Retry with the same failed UUID, should throw the same error.
    const failUUID = FailureTestClass.success === workflowUUID1 ? workflowUUID2 : workflowUUID1;
    try {
      await testRuntime.invoke(FailureTestClass, failUUID).testKeyConflict(10, failUUID);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      expect(err.code).toBe("23505");
      expect(err.table?.toLowerCase()).toBe(testTableName.toLowerCase());
    }
    // Retry with the succeed UUID, should return the expected result.
    await expect(testRuntime.invoke(FailureTestClass, FailureTestClass.success).testKeyConflict(10, FailureTestClass.success)).resolves.toStrictEqual({ id: 10 });
  });

  test("serialization-error", async () => {
    // Should succeed after retrying 10 times.
    await expect(
      testRuntime
        .invoke(FailureTestClass)
        .testSerialWorkflow(10)
        .then((x) => x.getResult())
    ).resolves.toBe(10);
    expect(FailureTestClass.cnt).toBe(10);
  });

  test("failing-communicator", async () => {
    await expect(testRuntime.invoke(FailureTestClass).testFailCommunicator()).resolves.toBe(4);

    await expect(testRuntime.invoke(FailureTestClass).testFailCommunicator()).rejects.toThrowError(new OperonError("Communicator reached maximum retries.", 1));
  });

  test("nonretry-communicator", async () => {
    const workflowUUID = uuidv1();

    // Should throw an error.
    await expect(testRuntime.invoke(FailureTestClass, workflowUUID).testNoRetry()).rejects.toThrowError(new Error("failed no retry"));
    expect(FailureTestClass.cnt).toBe(1);

    // If we retry again, we should get the same error, but numRun should still be 1 (OAOO).
    await expect(testRuntime.invoke(FailureTestClass, workflowUUID).testNoRetry()).rejects.toThrowError(new Error("failed no retry"));
    expect(FailureTestClass.cnt).toBe(1);
  });

  // eslint-disable-next-line @typescript-eslint/require-await
  test("no-registration", async () => {
    // Note: since we use invoke() in testing runtime, it throws "TypeError: ...is not a function" instead of OperonNotRegisteredError.

    // Invoke an unregistered workflow.
    expect(() => testRuntime.invoke(FailureTestClass).noRegWorkflow(10)).toThrowError();

    // Invoke an unregistered transaction.
    expect(() => testRuntime.invoke(FailureTestClass).noRegTransaction(10)).toThrowError();

    // Invoke an unregistered communicator in a workflow.
    await expect(testRuntime.invoke(FailureTestClass).testCommWorkflow().then(x => x.getResult())).rejects.toThrowError();
  });

  test("failure-recovery", async () => {
    // Run a workflow until pending and start recovery.
    const operon = testRuntime.getOperon();
    clearInterval(operon.flushBufferID); // Don't flush the output buffer.

    const handle = await testRuntime.invoke(FailureTestClass, undefined, { authenticatedUser: "test_recovery_user", request: { url: "test-recovery-url" } }).testRecoveryWorkflow(5);

    const recoverPromise = operon.recoverPendingWorkflows();
    FailureTestClass.resolve1();

    await recoverPromise;

    await expect(handle.getResult()).resolves.toBe("test_recovery_user");
    expect(FailureTestClass.cnt).toBe(10); // Should run twice.
  });
});

class FailureTestClass {
  static cnt = 0;
  static success: string = "";

  static resolve1: () => void;
  static promise1 = new Promise<void>((resolve) => {
    FailureTestClass.resolve1 = resolve;
  });

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonCommunicator({ retriesAllowed: false })
  static async testCommunicator(_ctxt: CommunicatorContext, @ArgOptional code?: number) {
    if (code) {
      throw new OperonError("test operon error with code.", code);
    } else {
      throw new OperonError("test operon error without code");
    }
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction({ readOnly: true })
  static async testReadonlyError(_txnCtxt: TestTransactionContext) {
    FailureTestClass.cnt++;
    throw new Error("test error");
  }

  @OperonTransaction()
  static async testKeyConflict(txnCtxt: TestTransactionContext, id: number, name: string) {
    const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName} (id, value) VALUES ($1, $2) RETURNING id`, [id, name]);
    FailureTestClass.cnt += 1;
    FailureTestClass.success = name;
    return rows[0];
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction()
  static async testSerialError(_ctxt: TestTransactionContext, maxRetry: number) {
    if (FailureTestClass.cnt !== maxRetry) {
      const err = new DatabaseError("serialization error", 10, "error");
      err.code = "40001";
      FailureTestClass.cnt += 1;
      throw err;
    }
    return maxRetry;
  }

  @OperonWorkflow()
  static async testSerialWorkflow(ctxt: WorkflowContext, maxRetry: number) {
    return await ctxt.invoke(FailureTestClass).testSerialError(maxRetry);
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonCommunicator({ intervalSeconds: 0, maxAttempts: 4 })
  static async testFailCommunicator(ctxt: CommunicatorContext) {
    FailureTestClass.cnt++;
    if (ctxt.retriesAllowed && FailureTestClass.cnt !== ctxt.maxAttempts) {
      throw new Error("bad number");
    }
    return FailureTestClass.cnt;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonCommunicator({ retriesAllowed: false })
  static async testNoRetry(_ctxt: CommunicatorContext) {
    FailureTestClass.cnt++;
    throw new Error("failed no retry");
  }

  // Test decorator registration works.
  // eslint-disable-next-line @typescript-eslint/require-await
  static async noRegComm(_ctxt: CommunicatorContext, code: number) {
    return code + 1;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  static async noRegTransaction(_ctxt: TestTransactionContext, code: number) {
    return code + 1;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  static async noRegWorkflow(_ctxt: WorkflowContext, code: number) {
    return code + 1;
  }

  @OperonWorkflow()
  static async testCommWorkflow(ctxt: WorkflowContext) {
    return await ctxt.invoke(FailureTestClass).noRegComm(1);
  }

  @OperonWorkflow()
  static async testRecoveryWorkflow(ctxt: WorkflowContext, input: number) {
    if (ctxt.authenticatedUser === "test_recovery_user" && ctxt.request.url === "test-recovery-url") {
      FailureTestClass.cnt += input;
    }
    await FailureTestClass.promise1;
    return ctxt.authenticatedUser;
  }
}
