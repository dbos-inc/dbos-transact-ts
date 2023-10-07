import { CommunicatorContext, Operon, OperonCommunicator, OperonTransaction, OperonWorkflow, TransactionContext, WorkflowContext } from "../src";
import { v1 as uuidv1 } from "uuid";
import { sleep } from "../src/utils";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { OperonConfig } from "../src/operon";
import { PoolClient } from "pg";

type TestTransactionContext = TransactionContext<PoolClient>;

describe("concurrency-tests", () => {
  let operon: Operon;
  const testTableName = "operon_concurrency_test_kv";

  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    operon = new Operon(config);
    await operon.init(ConcurrTestClass);
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`);
    ConcurrTestClass.cnt = 0;
    ConcurrTestClass.wfCnt = 0;
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("duplicate-transaction", async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workflowUUID = uuidv1();
    let results = await Promise.allSettled([
      operon.transaction(ConcurrTestClass.testReadWriteFunction, { workflowUUID: workflowUUID }, 10),
      operon.transaction(ConcurrTestClass.testReadWriteFunction, { workflowUUID: workflowUUID }, 10),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(10);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(10);
    expect(ConcurrTestClass.cnt).toBe(1);

    // Read-only transactions would execute twice.
    ConcurrTestClass.cnt = 0;

    const readUUID = uuidv1();
    results = await Promise.allSettled([
      operon.transaction(ConcurrTestClass.testReadOnlyFunction, { workflowUUID: readUUID }, 12),
      operon.transaction(ConcurrTestClass.testReadOnlyFunction, { workflowUUID: readUUID }, 12),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(12);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(12);
    expect(ConcurrTestClass.cnt).toBe(2);
  });

  test("concurrent-workflow", async () => {
    // Invoke testWorkflow twice with the same UUID and flush workflow output buffer right before the second transaction starts.
    // The second transaction should get the correct recorded execution without being executed.
    const uuid = uuidv1();
    await operon.workflow(ConcurrTestClass.testWorkflow, { workflowUUID: uuid }).then((x) => x.getResult());
    const handle = await operon.workflow(ConcurrTestClass.testWorkflow, { workflowUUID: uuid });
    await ConcurrTestClass.promise2;
    await operon.flushWorkflowStatusBuffer();
    ConcurrTestClass.resolve();
    await handle.getResult();

    expect(ConcurrTestClass.cnt).toBe(1);
    expect(ConcurrTestClass.wfCnt).toBe(2);
  });

  test("duplicate-communicator", async () => {
    // Run two communicators concurrently with the same UUID; both should succeed.
    // Since we only record the output after the function, it may cause more than once executions.
    const workflowUUID = uuidv1();
    const results = await Promise.allSettled([
      operon.external(ConcurrTestClass.testCommunicator, { workflowUUID: workflowUUID }, 11),
      operon.external(ConcurrTestClass.testCommunicator, { workflowUUID: workflowUUID }, 11),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(11);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(11);

    expect(ConcurrTestClass.cnt).toBeGreaterThanOrEqual(1);
  });

  test("duplicate-notifications", async () => {
    // Run two send/recv concurrently with the same UUID, both should succeed.
    // It's a bit hard to trigger conflicting send because the transaction runs quickly.
    const recvUUID = uuidv1();
    const recvResPromise = Promise.allSettled([
      operon.workflow(ConcurrTestClass.receiveWorkflow, { workflowUUID: recvUUID }, "testTopic", 2).then((x) => x.getResult()),
      operon.workflow(ConcurrTestClass.receiveWorkflow, { workflowUUID: recvUUID }, "testTopic", 2).then((x) => x.getResult()),
    ]);

    // Send would trigger both to receive, but only one can succeed.
    await sleep(10); // Both would be listening to the notification.

    await expect(operon.send(recvUUID, "testmsg", "testTopic")).resolves.toBeFalsy();

    const recvRes = await recvResPromise;
    expect((recvRes[0] as PromiseFulfilledResult<string | null>).value).toBe("testmsg");
    expect((recvRes[1] as PromiseFulfilledResult<string | null>).value).toBe("testmsg");

    const recvHandle = operon.retrieveWorkflow(recvUUID);
    await expect(recvHandle.getResult()).resolves.toBe("testmsg");
  });
});

class ConcurrTestClass {
  static cnt = 0;
  static wfCnt = 0;
  static resolve: () => void;
  static promise = new Promise<void>((r) => {
    ConcurrTestClass.resolve = r;
  });

  static resolve2: () => void;
  static promise2 = new Promise<void>((r) => {
    ConcurrTestClass.resolve2 = r;
  });

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction()
  static async testReadWriteFunction(_txnCtxt: TestTransactionContext, id: number) {
    ConcurrTestClass.cnt++;
    return id;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction({ readOnly: true })
  static async testReadOnlyFunction(_txnCtxt: TestTransactionContext, id: number) {
    ConcurrTestClass.cnt += 1;
    return id;
  }

  @OperonWorkflow()
  static async testWorkflow(ctxt: WorkflowContext) {
    if (ConcurrTestClass.wfCnt++ === 1) {
      ConcurrTestClass.resolve2();
      await ConcurrTestClass.promise;
    }
    await ctxt.invoke(ConcurrTestClass).testReadWriteFunction(1);
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonCommunicator()
  static async testCommunicator(_ctxt: CommunicatorContext, id: number) {
    ConcurrTestClass.cnt++;
    return id;
  }

  @OperonWorkflow()
  static async receiveWorkflow(ctxt: WorkflowContext, topic: string, timeout: number) {
    return ctxt.recv<string>(topic, timeout);
  }
}
