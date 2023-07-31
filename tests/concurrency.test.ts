import { CommunicatorContext, Operon, OperonError, TransactionContext, WorkflowContext } from "src/";
import { v1 as uuidv1 } from 'uuid';
import { sleep } from "./helper";

describe('concurrency-tests', () => {
  let operon: Operon;
  const testTableName = 'OperonConcurrencyTestKv';

  beforeEach(async () => {
    operon = new Operon();
    await operon.resetOperonTables();
    await operon.pool.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.pool.query(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test('duplicate-transaction',async () => {
    // Run two transactions concurrently with the same UUID.
    // Only one should succeed, and the other one must fail.
    const testFunction = async (txnCtxt: TransactionContext, id: number, sleepMs: number=0) => {
      await sleep(sleepMs);
      return id;
    };
    operon.registerTransaction(testFunction, {});

    const workflowUUID = uuidv1();
    let results = await Promise.allSettled([
      operon.transaction(testFunction, {workflowUUID: workflowUUID}, 10, 10),
      operon.transaction(testFunction, {workflowUUID: workflowUUID}, 10, 10)
    ]);
    const errorResult = results.find(result => result.status === 'rejected');
    const goodResult = results.find(result => result.status === 'fulfilled');
    expect((goodResult as PromiseFulfilledResult<number>).value).toBe(10);
    const err: OperonError = (errorResult as PromiseRejectedResult).reason as OperonError;
    expect(err.message).toBe('Conflicting UUIDs');

    // If we mark the function as read-only, both should succeed.
    operon.registerTransaction(testFunction, {readOnly: true});
    const readUUID = uuidv1();
    results = await Promise.allSettled([
      operon.transaction(testFunction, {workflowUUID: readUUID}, 12, 10),
      operon.transaction(testFunction, {workflowUUID: readUUID}, 12, 10)
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(12);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(12);
  });

  test('duplicate-communicator',async () => {
    // Run two communicators concurrently with the same UUID.
    // Since we only record the output after the function, it may cause more than once executions.
    // However, only one should return successfully.
    const remoteState = {
      cnt: 0
    };

    const testFunction = async (ctxt: CommunicatorContext, counter: number, sleepMs: number=0) => {
      await sleep(sleepMs);
      remoteState.cnt += 1;
      return counter;
    };
    operon.registerCommunicator(testFunction, {retriesAllowed: false});

    const testWorkflow = async (workflowCtxt: WorkflowContext, counter: number, sleepMs: number=0) => {
      const funcResult = await workflowCtxt.external(testFunction, counter, sleepMs);
      return funcResult ?? "error";
    };
    operon.registerWorkflow(testWorkflow);

    const workflowUUID = uuidv1();
    const results = await Promise.allSettled([
      operon.workflow(testWorkflow, {workflowUUID: workflowUUID}, 11, 10),
      operon.workflow(testWorkflow, {workflowUUID: workflowUUID}, 11, 10)
    ]);
    const errorResult = results.find(result => result.status === 'rejected');
    const goodResult = results.find(result => result.status === 'fulfilled');
    expect((goodResult as PromiseFulfilledResult<number>).value).toBe(11);
    const err: OperonError = (errorResult as PromiseRejectedResult).reason as OperonError;
    expect(err.message).toBe('Conflicting UUIDs');

    // But the communicator function still runs twice as we do not guarantee OAOO.
    expect(remoteState.cnt).toBe(2);
  });

});