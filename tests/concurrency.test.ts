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
    // Since we put a guard before each transaction, only one should proceed.
    const remoteState = {
      cnt: 0
    };
    const testFunction = async (txnCtxt: TransactionContext, id: number, sleepMs: number=0) => {
      await sleep(sleepMs);
      remoteState.cnt += 1;
      return id;
    };
    operon.registerTransaction(testFunction, {});

    const workflowUUID = uuidv1();
    let results = await Promise.allSettled([
      operon.transaction(testFunction, {workflowUUID: workflowUUID}, 10, 100),
      operon.transaction(testFunction, {workflowUUID: workflowUUID}, 10, 100)
    ]);
    const errorResult = results.find(result => result.status === 'rejected');
    const goodResult = results.find(result => result.status === 'fulfilled');
    expect((goodResult as PromiseFulfilledResult<number>).value).toBe(10);
    const err: OperonError = (errorResult as PromiseRejectedResult).reason as OperonError;
    expect(err.message).toBe('Conflicting UUIDs');
    expect(remoteState.cnt).toBe(1);

    // If we mark the function as read-only, both should succeed.
    remoteState.cnt = 0;
    operon.registerTransaction(testFunction, {readOnly: true});
    const readUUID = uuidv1();
    results = await Promise.allSettled([
      operon.transaction(testFunction, {workflowUUID: readUUID}, 12, 10),
      operon.transaction(testFunction, {workflowUUID: readUUID}, 12, 10)
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(12);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(12);
    expect(remoteState.cnt).toBe(2);
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

  test('duplicate-notifications',async () => {
    // Run two send/recv concurrently with the same UUID, only one can succeed.
    // It's a bit hard to trigger conflicting send because the transaction runs quickly.
    const recvUUID = uuidv1();
    const recvResPromise = Promise.allSettled([
      operon.recv({workflowUUID: recvUUID}, "testmsg", 2),
      operon.recv({workflowUUID: recvUUID}, "testmsg", 2)
    ]);

    // Send would trigger both to receive, but only one can succeed.
    await sleep(10); // Both would be listening to the notification.
    await expect(operon.send({}, "testmsg", "hello")).resolves.toBe(true);
    const recvRes = await recvResPromise;
    const recvErr = recvRes.find(result => result.status === 'rejected');
    const recvGood = recvRes.find(result => result.status === 'fulfilled');
    expect((recvGood as PromiseFulfilledResult<boolean>).value).toBe("hello");
    const err = (recvErr as PromiseRejectedResult).reason as OperonError;
    expect(err.message).toBe('Conflicting UUIDs');
  });

});