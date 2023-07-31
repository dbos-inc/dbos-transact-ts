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
} from './helpers';
import { DatabaseError } from "pg";
import { v1 as uuidv1 } from 'uuid';

interface KvTable {
  id?: number,
  value?: string,
}

// Sleep for specified milliseconds.
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

describe('concurrency-tests', () => {
  let operon: Operon;
  const testTableName = 'OperonConcurrentKv';
  let config: OperonConfig;

  beforeAll(() => {
    config = generateOperonTestConfig();
  });

  afterAll(async () => {
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
      await sleep(1);
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

    await expect(operon.workflow(testWorkflow, {}, 11)).rejects.toThrowError(new OperonError("test operon error with code.", 11));

    // Test without code.
    await expect(operon.workflow(testWorkflow, {})).rejects.toThrowError(new OperonError("test operon error without code"));
  });

  test('simple-keyconflict', async() => {
    let counter: number = 0;
    let succeedUUID: string = '';
    const testFunction = async (txnCtxt: TransactionContext, id: number, name: string) => {
      const { rows } = await txnCtxt.client.query<KvTable>(`INSERT INTO ${testTableName} VALUES ($1, $2) RETURNING id`, [id, name]);
      await sleep(10);
      counter += 1;
      succeedUUID = name;
      return rows[0];
    };
    operon.registerTransaction(testFunction);

    const workflowUUID1 = uuidv1();
    const workflowUUID2 = uuidv1();
    try {
      // Start two concurrent transactions.
      const futRes1 = operon.transaction(testFunction, {workflowUUID: workflowUUID1}, 10, workflowUUID1);
      const futRes2 = operon.transaction(testFunction, {workflowUUID: workflowUUID2}, 10, workflowUUID2);
      await futRes1;
      await futRes2;
    } catch (error) {
      expect(error).toBeInstanceOf(DatabaseError);
      const err: DatabaseError = error as DatabaseError;
      // Expect to throw a database error for primary key violation.
      expect(err.code).toBe('23505');
    }

    expect(counter).toBe(1);

    // Retry with the same failed UUID, should throw the same error.
    const failUUID = (succeedUUID === workflowUUID1) ? workflowUUID2 : workflowUUID1;
    await expect(operon.transaction(testFunction, {workflowUUID: failUUID}, 10, failUUID)).rejects.toThrow(DatabaseError);

    // Retry with the succeed UUID, should return the expected result.
    await expect(operon.transaction(testFunction, {workflowUUID: succeedUUID}, 10, succeedUUID)).resolves.toStrictEqual({"id": 10});
  });

  test('serialization-error', async() => {
    // Just for testing, functions shouldn't share global state.
    const remoteState = {
      num: 0
    }
    const testFunction = async (txnCtxt: TransactionContext, maxRetry: number) => {
      if (remoteState.num !== maxRetry) {
        const err = new DatabaseError("serialization error", 10, "error");
        err.code = '40001';
        remoteState.num += 1;
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
    await expect(operon.workflow(testWorkflow, {}, 10)).resolves.toBe(10);
    expect(remoteState.num).toBe(10);
  });

  test('failing-communicator', async() => {

    const remoteState = {
      num: 0
    }

    const testCommunicator = async (ctxt: CommunicatorContext) => {
      remoteState.num += 1;
      if (remoteState.num !== ctxt.maxAttempts) {
        throw new Error("bad number");
      }
      await sleep(10);
      return remoteState.num;
    };
    operon.registerCommunicator(testCommunicator, {intervalSeconds: 0, maxAttempts: 4});

    const testWorkflow = async (ctxt: WorkflowContext) => {
      return await ctxt.external(testCommunicator);
    };
    operon.registerWorkflow(testWorkflow);
  
    const result = await operon.workflow(testWorkflow, {});
    expect(result).toEqual(4);

    await expect(operon.workflow(testWorkflow, {})).rejects.toThrowError(new OperonError("Communicator reached maximum retries.", 1));

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
    await expect(operon.workflow(testWorkflow, {workflowUUID: workflowUUID})).rejects.toThrowError(new Error("failed no retry"));
    expect(numRun).toBe(1);

    // If we retry again, we should get the same error, but numRun should still be 1 (OAOO).
    await expect(operon.workflow(testWorkflow, {workflowUUID: workflowUUID})).rejects.toThrowError(new Error("failed no retry"));
    expect(numRun).toBe(1);
  });
});
