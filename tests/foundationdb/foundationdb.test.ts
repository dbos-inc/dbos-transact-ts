import {
  Operon,
  OperonConfig,
  TransactionContext,
  CommunicatorContext,
  WorkflowContext,
  StatusString,
} from "src/";
import { generateOperonTestConfig, setupOperonTestDb } from "../helpers";
import { FoundationDBSystemDatabase } from "src/foundationdb/fdb_system_database";
import { v1 as uuidv1 } from "uuid";

describe("foundationdb-operon", () => {
  let operon: Operon;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    const systemDB: FoundationDBSystemDatabase =
      new FoundationDBSystemDatabase();
    operon = new Operon(config, systemDB);
    operon.useNodePostgres();
    await operon.init();
    // Clean up tables.
    await systemDB.workflowStatusDB.clearRangeStartsWith("");
    await systemDB.operationOutputsDB.clearRangeStartsWith([]);
    await systemDB.notificationsDB.clearRangeStartsWith([]);
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("fdb-operon", async () => {
    let counter = 0;

    // eslint-disable-next-line @typescript-eslint/require-await
    const testFunction = async (txnCtxt: TransactionContext) => {
      void txnCtxt;
      counter++;
      return 5;
    };
    operon.registerTransaction(testFunction);
    const uuid = uuidv1();
    await expect(
      operon.transaction(testFunction, { workflowUUID: uuid })
    ).resolves.toBe(5);
    await operon.flushWorkflowStatusBuffer();
    await expect(
      operon.transaction(testFunction, { workflowUUID: uuid })
    ).resolves.toBe(5);
    expect(counter).toBe(1);
  });

  test("fdb-error-recording", async () => {
    let counter = 0;
    // eslint-disable-next-line @typescript-eslint/require-await
    const testFunction = async (txnCtxt: TransactionContext) => {
      void txnCtxt;
      if (counter++ === 0) {
        throw new Error("fail");
      }
    };
    operon.registerTransaction(testFunction);
    const uuid = uuidv1();
    await expect(
      operon.transaction(testFunction, { workflowUUID: uuid })
    ).rejects.toThrow("fail");
    await operon.flushWorkflowStatusBuffer();
    await expect(
      operon.transaction(testFunction, { workflowUUID: uuid })
    ).rejects.toThrow("fail");
    expect(counter).toBe(1);
  });

  test("fdb-communicator", async () => {
    let counter = 0;
    // eslint-disable-next-line @typescript-eslint/require-await
    const testCommunicator = async (commCtxt: CommunicatorContext) => {
      void commCtxt;
      return counter++;
    };
    operon.registerCommunicator(testCommunicator);

    const testWorkflow = async (workflowCtxt: WorkflowContext) => {
      const funcResult = await workflowCtxt.external(testCommunicator);
      return funcResult;
    };
    operon.registerWorkflow(testWorkflow);
    const workflowUUID: string = uuidv1();

    await expect(
      operon.workflow(testWorkflow, { workflowUUID: workflowUUID }).getResult()
    ).resolves.toBe(0);

    // Test OAOO. Should return the original result.
    await expect(
      operon.workflow(testWorkflow, { workflowUUID: workflowUUID }).getResult()
    ).resolves.toBe(0);
    expect(counter).toBe(1);
  });

  test("fdb-communicator-error", async () => {
    let num = 0;

    // eslint-disable-next-line @typescript-eslint/require-await
    const testCommunicator = async (ctxt: CommunicatorContext) => {
      num += 1;
      if (num !== ctxt.maxAttempts) {
        throw new Error("bad number");
      }
      return num;
    };
    operon.registerCommunicator(testCommunicator, {
      intervalSeconds: 0,
      maxAttempts: 4,
    });

    const testWorkflow = async (ctxt: WorkflowContext) => {
      try {
        await ctxt.external(testCommunicator);
      } catch (err) {
        return (err as Error).message;
      }
      return "success";
    };
    operon.registerWorkflow(testWorkflow);

    await expect(operon.workflow(testWorkflow, {}).getResult()).resolves.toBe(
      "success"
    );

    const workflowUUID: string = uuidv1();
    await expect(
      operon.workflow(testWorkflow, { workflowUUID: workflowUUID }).getResult()
    ).resolves.toBe("Communicator reached maximum retries.");
    await expect(
      operon.workflow(testWorkflow, { workflowUUID: workflowUUID }).getResult()
    ).resolves.toBe("Communicator reached maximum retries.");
  });

  test("fdb-workflow-status", async () => {
    let counter = 0;
    
    let innerResolve: () => void;
    const innerPromise = new Promise<void>((r) => {
      innerResolve = r;
    });

    let outerResolve: () => void;
    const outerPromise = new Promise<void>((r) => {
      outerResolve = r;
    });

    // eslint-disable-next-line @typescript-eslint/require-await
    const testFunction = async (txnCtxt: TransactionContext) => {
      void txnCtxt;
      counter++;
      return 3;
    };
    const testWorkflow = async (ctxt: WorkflowContext) => {
      const result = ctxt.transaction(testFunction);
      outerResolve();
      await(innerPromise);
      return result;
    };
    operon.registerTransaction(testFunction);
    operon.registerWorkflow(testWorkflow);

    const uuid = uuidv1();
    const invokedHandle = operon.workflow(testWorkflow, { workflowUUID: uuid });
    await outerPromise;
    await operon.systemDatabase.flushWorkflowStatusBuffer();
    const retrievedHandle = operon.retrieveWorkflow(uuid);
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
    });
    innerResolve!();
    await expect(invokedHandle.getResult()).resolves.toBe(3);
    await operon.systemDatabase.flushWorkflowStatusBuffer();
    await expect(retrievedHandle.getResult()).resolves.toBe(3);
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
    expect(counter).toBe(1);
  });

  test("fdb-notifications", async () => {
    const receiveWorkflow = async (ctxt: WorkflowContext) => {
      const message1 = await ctxt.recv<string>();
      const message2 = await ctxt.recv<string>();
      const fail = await ctxt.recv("fail", 0);
      return message1 === "message1" && message2 === "message2" && fail === null;
    };
    operon.registerWorkflow(receiveWorkflow);

    const sendWorkflow = async (ctxt: WorkflowContext, destinationUUID: string) => {
      await ctxt.send(destinationUUID, "message1");
      await ctxt.send(destinationUUID, "message2");
    };
    operon.registerWorkflow(sendWorkflow);

    const workflowUUID = uuidv1();
    const handle = operon.workflow(receiveWorkflow, { workflowUUID: workflowUUID });
    await operon.workflow(sendWorkflow, {}, handle.getWorkflowUUID()).getResult();
    expect(await handle.getResult()).toBe(true);
    const retry = await operon.workflow(receiveWorkflow, { workflowUUID: workflowUUID }).getResult();
    expect(retry).toBe(true);
  });

  test("fdb-duplicate-communicator", async () => {
    // Run two communicators concurrently with the same UUID; both should succeed.
    // Since we only record the output after the function, it may cause more than once executions.
    let counter = 0;

    // eslint-disable-next-line @typescript-eslint/require-await
    const testFunction = async (ctxt: CommunicatorContext, id: number) => {
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
        .getResult(),
      operon
        .workflow(testWorkflow, { workflowUUID: workflowUUID }, 11)
        .getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(11);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(11);

    expect(counter).toBeGreaterThanOrEqual(1);
  });

  test("fdb-duplicate-notifications", async () => {
    const receiveWorkflow = async (ctxt: WorkflowContext, topic: string, timeout: number) => {
      return ctxt.recv<string>(topic, timeout);
    };
    operon.registerWorkflow(receiveWorkflow);

    // Run two send/recv concurrently with the same UUID, both should succeed.
    const recvUUID = uuidv1();
    const sendUUID = uuidv1();
    const recvResPromise = Promise.allSettled([
      operon.workflow(receiveWorkflow, { workflowUUID: recvUUID }, "testTopic", 2).getResult(),
      operon.workflow(receiveWorkflow, { workflowUUID: recvUUID }, "testTopic", 2).getResult(),
    ]);

    // Send would trigger both to receive, but only one can delete the message.
    await expect(
      operon.send({ workflowUUID: sendUUID }, recvUUID, "hello", "testTopic")
    ).resolves.not.toThrow();

    const recvRes = await recvResPromise;
    expect((recvRes[0] as PromiseFulfilledResult<string>).value).toBe("hello");
    expect((recvRes[1] as PromiseFulfilledResult<string>).value).toBe("hello");

    // Make sure we retrieve results correctly.
    await expect(operon.retrieveWorkflow(sendUUID).getResult()).resolves.not.toThrow();
    await expect(operon.retrieveWorkflow(recvUUID).getResult()).resolves.toBe(
      "hello"
    );
  });
});

