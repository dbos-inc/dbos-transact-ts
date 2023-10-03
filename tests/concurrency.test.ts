import {
  CommunicatorContext,
  Operon,
  TransactionContext,
  WorkflowContext,
} from "../src";
import { v1 as uuidv1 } from "uuid";
import { sleep } from "../src/utils";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { OperonConfig } from "../src/operon";

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
    await operon.init();
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`
    );
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("duplicate-transaction", async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    let cnt = 0;
    const testReadWriteFunction = async (txnCtxt: TransactionContext, id: number) => {
      await sleep(10);
      cnt += 1;
      return id;
    };
    operon.registerTransaction(testReadWriteFunction);

    const workflowUUID = uuidv1();
    let results = await Promise.allSettled([
      operon.transaction(testReadWriteFunction, { workflowUUID: workflowUUID }, 10),
      operon.transaction(testReadWriteFunction, { workflowUUID: workflowUUID }, 10),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(10);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(10);
    expect(cnt).toBe(1);

    // Read-only transactions would execute twice.
    cnt = 0;
    const testReadOnlyFunction = async (txnCtxt: TransactionContext, id: number) => {
      await sleep(10);
      cnt += 1;
      return id;
    };
    operon.registerTransaction(testReadOnlyFunction, { readOnly: true });
    const readUUID = uuidv1();
    results = await Promise.allSettled([
      operon.transaction(testReadOnlyFunction, { workflowUUID: readUUID }, 12),
      operon.transaction(testReadOnlyFunction, { workflowUUID: readUUID }, 12),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(12);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(12);
    expect(cnt).toBe(2);
  });

  test("concurrent-gc", async () => {
    let resolve: () => void;
    const promise = new Promise<void>((r) => {
      resolve = r;
    });

    let resolve2: () => void;
    const promise2 = new Promise<void>((r) => {
      resolve2 = r;
    });

    let wfCounter = 0;
    let funCounter = 0;

    // Invoke testWorkflow twice with the same UUID and flush workflow output buffer right before the second transaction starts.
    // The second transaction should get the correct recorded execution without being executed.
    const testWorkflow = async (ctxt: WorkflowContext) => {
      if (wfCounter++ === 1) {
        resolve2!();
        await promise;
      }
      await ctxt.transaction(testFunction);
    };
    operon.registerWorkflow(testWorkflow);

    const testFunction = async (ctxt: TransactionContext) => {
      void ctxt;
      await sleep(1);
      funCounter++;
      return;
    };
    operon.registerTransaction(testFunction);

    const uuid = uuidv1();
    await operon.workflow(testWorkflow, { workflowUUID: uuid }).then(x => x.getResult());
    const handle = await operon.workflow(testWorkflow, { workflowUUID: uuid });
    await promise2;
    await operon.flushWorkflowStatusBuffer();
    resolve!();
    await handle.getResult();

    expect(funCounter).toBe(1);
    expect(wfCounter).toBe(2);
  });

  test("duplicate-communicator", async () => {
    // Run two communicators concurrently with the same UUID; both should succeed.
    // Since we only record the output after the function, it may cause more than once executions.
    let counter = 0;

    const testFunction = async (ctxt: CommunicatorContext, id: number) => {
      await sleep(10);
      counter++;
      void ctxt;
      return id;
    };
    operon.registerCommunicator(testFunction, { retriesAllowed: false });

    const testWorkflow = async (workflowCtxt: WorkflowContext, id: number) => {
      const funcResult = await workflowCtxt.external(testFunction, id);
      return funcResult ?? -1;
    };
    operon.registerWorkflow(testWorkflow);

    const workflowUUID = uuidv1();
    const results = await Promise.allSettled([
      operon
        .workflow(testWorkflow, { workflowUUID: workflowUUID }, 11)
        .then(x => x.getResult()),
      operon
        .workflow(testWorkflow, { workflowUUID: workflowUUID }, 11)
        .then(x => x.getResult()),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(11);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(11);

    expect(counter).toBeGreaterThanOrEqual(1);
  });

  test("duplicate-notifications", async () => {
    const receiveWorkflow = async (ctxt: WorkflowContext, topic: string, timeout: number) => {
      return ctxt.recv<string>(topic, timeout);
    };
    operon.registerWorkflow(receiveWorkflow);
    // Run two send/recv concurrently with the same UUID, both should succeed.
    // It's a bit hard to trigger conflicting send because the transaction runs quickly.
    const recvUUID = uuidv1();
    const sendUUID = uuidv1();
    const recvResPromise = Promise.allSettled([
      operon.workflow(receiveWorkflow, { workflowUUID: recvUUID }, "testTopic", 2).then(x => x.getResult()),
      operon.workflow(receiveWorkflow, { workflowUUID: recvUUID }, "testTopic", 2).then(x => x.getResult()),
    ]);

    // Send would trigger both to receive, but only one can succeed.
    await sleep(10); // Both would be listening to the notification.

    await expect(
      operon.send({ workflowUUID: sendUUID }, recvUUID, "testmsg", "testTopic")
    ).resolves.toBeFalsy();

    const recvRes = await recvResPromise;
    expect((recvRes[0] as PromiseFulfilledResult<string | null>).value).toBe("testmsg");
    expect((recvRes[1] as PromiseFulfilledResult<string | null>).value).toBe("testmsg");

    // Make sure we retrieve results correctly.
    const sendHandle = operon.retrieveWorkflow(sendUUID);
    await expect(sendHandle.getResult()).resolves.toBeFalsy();

    const recvHandle = operon.retrieveWorkflow(recvUUID);
    await expect(recvHandle.getResult()).resolves.toBe("testmsg");
  });
});
