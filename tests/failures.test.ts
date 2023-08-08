import {
  Operon,
  OperonConfig,
  WorkflowContext,
  TransactionContext,
  OperonError,
  CommunicatorContext,
} from "src/";
import {
  generateOperonTestConfig,
  teardownOperonTestDb,
  TestKvTable
} from './helpers';
import { DatabaseError } from "pg";
import { v1 as uuidv1 } from 'uuid';
import { sleep } from "src/utils";
import { StatusString } from "src/workflow";

describe('failures-tests', () => {
  let operon: Operon;

  const testTableName = 'operon_failure_test_kv';
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await teardownOperonTestDb(config);
  });

  beforeEach(async () => {
    operon = new Operon(config);
    await operon.init();
    await operon.pool.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.pool.query(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test('operon-error', async() => {
    const testCommunicator = async (ctxt: CommunicatorContext, code?: number) => {
      void ctxt;
      await sleep(10);
      if (code) {
        throw new OperonError("test operon error with code.", code);
      } else {
        throw new OperonError("test operon error without code");
      }
    };
    operon.registerCommunicator(testCommunicator, {retriesAllowed: false});

    const testWorkflow = async (ctxt: WorkflowContext, code?: number) => {
      return await ctxt.external(testCommunicator, code);
    };
    operon.registerWorkflow(testWorkflow);

    const codeHandle = operon.workflow(testWorkflow, {}, 11);
    await expect(codeHandle.getResult()).rejects.toThrowError(new OperonError("test operon error with code.", 11));
    await expect(codeHandle.getStatus()).resolves.toMatchObject({status: StatusString.ERROR});
    const retrievedHandle = operon.retrieveWorkflow<string>(codeHandle.getWorkflowUUID());
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({status: StatusString.ERROR});
    await expect(retrievedHandle.getResult()).rejects.toThrowError(new OperonError("test operon error with code.", 11));

    // Test without code.
    const wfUUID = uuidv1();
    const noCodeHandle = operon.workflow(testWorkflow, {workflowUUID: wfUUID});
    await expect(noCodeHandle.getResult()).rejects.toThrowError(new OperonError("test operon error without code"));
    expect(noCodeHandle.getWorkflowUUID()).toBe(wfUUID);
    await expect(noCodeHandle.getStatus()).resolves.toMatchObject({status: StatusString.ERROR});
  });

  test('readonly-error', async() => {
    let cnt = 0;

    const testFunction = async (txnCtxt: TransactionContext, id: number) => {
      await sleep(1);
      void txnCtxt;
      cnt += 1;
      throw new Error("test error");
      return id;
    };
    operon.registerTransaction(testFunction, {readOnly: true});

    const testUUID = uuidv1();
    await expect(operon.transaction(testFunction, {workflowUUID: testUUID}, 11)).rejects.toThrowError(new Error("test error"));
    expect(cnt).toBe(1);

    // The error should be recorded in the database, so the function shouldn't run again.
    await expect(operon.transaction(testFunction, {workflowUUID: testUUID}, 11)).rejects.toThrowError(new Error("test error"));
    expect(cnt).toBe(1);
  });

  test('simple-keyconflict', async() => {
    let counter: number = 0;
    let succeedUUID: string = '';
    const testFunction = async (txnCtxt: TransactionContext, id: number, name: string) => {
      const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName} (id, value) VALUES ($1, $2) RETURNING id`, [id, name]);
      counter += 1;
      succeedUUID = name;
      return rows[0];
    };
    operon.registerTransaction(testFunction);

    const workflowUUID1 = uuidv1();
    const workflowUUID2 = uuidv1();

    // Start two concurrent transactions.
    const results = await Promise.allSettled([
      operon.transaction(testFunction, {workflowUUID: workflowUUID1}, 10, workflowUUID1),
      operon.transaction(testFunction, {workflowUUID: workflowUUID2}, 10, workflowUUID2)
    ]);
    const errorResult = results.find(result => result.status === 'rejected');
    const err: DatabaseError = (errorResult as PromiseRejectedResult).reason as DatabaseError;
    expect(err.code).toBe('23505');
    expect(err.table?.toLowerCase()).toBe(testTableName.toLowerCase());

    expect(counter).toBe(1);

    // Retry with the same failed UUID, should throw the same error.
    const failUUID = (succeedUUID === workflowUUID1) ? workflowUUID2 : workflowUUID1;
    try {
      await operon.transaction(testFunction, {workflowUUID: failUUID}, 10, failUUID);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      expect(err.code).toBe('23505');
      expect(err.table?.toLowerCase()).toBe(testTableName.toLowerCase());
    }
    // Retry with the succeed UUID, should return the expected result.
    await expect(operon.transaction(testFunction, {workflowUUID: succeedUUID}, 10, succeedUUID)).resolves.toStrictEqual({"id": 10});
  });

  test('serialization-error', async() => {
    // Just for testing, functions shouldn't share global state.
    let num = 0;
    const testFunction = async (txnCtxt: TransactionContext, maxRetry: number) => {
      if (num !== maxRetry) {
        const err = new DatabaseError("serialization error", 10, "error");
        err.code = '40001';
        num += 1;
        throw err;
      }
      await sleep(1);
      return maxRetry;
    };
    operon.registerTransaction(testFunction);

    const testWorkflow = async (ctxt: WorkflowContext, maxRetry: number) => {
      return await ctxt.transaction(testFunction, maxRetry);
    };
    operon.registerWorkflow(testWorkflow);

    // Should succeed after retrying 10 times.
    await expect(operon.workflow(testWorkflow, {}, 10).getResult()).resolves.toBe(10);
    expect(num).toBe(10);
  });

  test('failing-communicator', async() => {
    let num = 0;

    const testCommunicator = async (ctxt: CommunicatorContext) => {
      num += 1;
      if (num !== ctxt.maxAttempts) {
        throw new Error("bad number");
      }
      await sleep(1);
      return num;
    };
    operon.registerCommunicator(testCommunicator, {intervalSeconds: 0, maxAttempts: 4});

    const testWorkflow = async (ctxt: WorkflowContext) => {
      return await ctxt.external(testCommunicator);
    };
    operon.registerWorkflow(testWorkflow);
  
    await expect(operon.workflow(testWorkflow, {}).getResult()).resolves.toBe(4);

    await expect(operon.workflow(testWorkflow, {}).getResult()).rejects.toThrowError(new OperonError("Communicator reached maximum retries.", 1));
  });

  test('nonretry-communicator', async () => {
    let numRun: number = 0;
    const testCommunicator = async (ctxt: CommunicatorContext): Promise<number> => {
      await sleep(1);
      void ctxt;
      numRun += 1;
      throw new Error("failed no retry");
      return 10;
    };
    operon.registerCommunicator(testCommunicator, { retriesAllowed: false });

    const testWorkflow = async (ctxt: WorkflowContext): Promise<number> => {
      void ctxt;
      return await ctxt.external(testCommunicator);
    };
    operon.registerWorkflow(testWorkflow);

    const workflowUUID = uuidv1();

    // Should throw an error.
    await expect(operon.workflow(testWorkflow, {workflowUUID: workflowUUID}).getResult()).rejects.toThrowError(new Error("failed no retry"));
    expect(numRun).toBe(1);

    // If we retry again, we should get the same error, but numRun should still be 1 (OAOO).
    await expect(operon.workflow(testWorkflow, {workflowUUID: workflowUUID}).getResult()).rejects.toThrowError(new Error("failed no retry"));
    expect(numRun).toBe(1);
  });

  test('no-registration',async () => {
    const testFunction = async (txnCtxt: TransactionContext, id: number, name: string) => {
      const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName} (id, value) VALUES ($1, $2) RETURNING id`, [id, name]);
      if (rows.length === 0) {
        return null;
      }
      return rows[0].id;
    };

    const testCommunicator = async (ctxt: CommunicatorContext, code: number) => {
      void ctxt;
      await sleep(1);
      return code + 1;
    };

    const testWorkflow = async (ctxt: WorkflowContext, id: number, name: string) => {
      const resId = await ctxt.external(testCommunicator, id);
      return await ctxt.transaction(testFunction, resId, name);
    };

    // Invoke an unregistered workflow.
    await expect(operon.workflow(testWorkflow, {}, 10, "test").getResult()).rejects.toThrowError(new OperonError(`Unregistered Workflow ${testWorkflow.name}`));

    // Invoke an unregistered transaction.
    await expect(operon.transaction(testFunction, {}, 10, "test")).rejects.toThrowError(new OperonError(`Unregistered Transaction ${testFunction.name}`));

    operon.registerTransaction(testFunction, {});
    operon.registerWorkflow(testWorkflow, {});

    // Invoke an unregistered communicator.
    await expect(operon.workflow(testWorkflow, {}, 10, "test").getResult()).rejects.toThrowError(new OperonError(`Unregistered External ${testCommunicator.name}`));

    operon.registerCommunicator(testCommunicator, {});

    // Now everything should work.
    await expect(operon.workflow(testWorkflow, {}, 10, "test").getResult()).resolves.toBe(11);
  });

  test('failure-recovery', async() => {
    // Run a workflow until it reaches PENDING state, then shut down the server, and recover from there.
    let resolve1: () => void;
    const promise1 = new Promise<void>((resolve) => {
      resolve1 = resolve;
    });

    let resolve2: () => void;
    const promise2 = new Promise<void>((resolve) => {
      resolve2 = resolve;
    });

    const writeFunction = async (txnCtxt: TransactionContext, id: number, name: string) => {
      const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName} (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value=EXCLUDED.value RETURNING value;`, [id, name]);
      return rows[0].value!;
    };
    operon.registerTransaction(writeFunction, {});

    const testWorkflow = async (workflowCtxt: WorkflowContext, id: number, name: string) => {
      const value = await workflowCtxt.transaction(writeFunction, id, name);
      resolve1();  // Signal the execution has done.
      await promise2;
      return value;
    };
    operon.registerWorkflow(testWorkflow, {});

    const workflowUUID = uuidv1();

    const invokeHandle = operon.workflow(testWorkflow,  {workflowUUID: workflowUUID}, 123, "hello");

    await promise1;

    // Now should see the pending state.
    await expect(invokeHandle.getStatus()).resolves.toMatchObject({status: StatusString.PENDING});

    // Shut down the server.
    await operon.destroy();

    await sleep(1000);

    // Create a new operon and register everything
    operon = new Operon(config);
    await operon.init();
    operon.registerTransaction(writeFunction, {});
    operon.registerWorkflow(testWorkflow, {});

    // Start the recovery.
    resolve2!();
    await operon.recoverPendingWorkflows();
    await expect(operon.retrieveWorkflow<string>(workflowUUID).getResult()).resolves.toBe("hello");
  });
});
