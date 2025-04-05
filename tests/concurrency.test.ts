import { DBOS } from '../src';
import { v1 as uuidv1 } from 'uuid';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';

const testTableName = 'dbos_concurrency_test_kv';

describe('concurrency-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`);
    ConcurrTestClass.cnt = 0;
    ConcurrTestClass.wfCnt = 0;
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('duplicate-transaction', async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workflowUUID = uuidv1();
    const results = await Promise.allSettled([
      (await DBOS.startWorkflow(ConcurrTestClass, { workflowID: workflowUUID }).testReadWriteFunction(10)).getResult(),
      (await DBOS.startWorkflow(ConcurrTestClass, { workflowID: workflowUUID }).testReadWriteFunction(10)).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(10);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(10);
    expect(ConcurrTestClass.cnt).toBe(1);
  });

  test('concurrent-workflow', async () => {
    // Invoke testWorkflow twice with the same UUID and the first one completes right before the second transaction starts.
    // The second transaction should get the correct recorded execution without being executed.
    const uuid = uuidv1();
    const handle1 = await DBOS.startWorkflow(ConcurrTestClass, { workflowID: uuid }).testWorkflow();
    const handle2 = await DBOS.startWorkflow(ConcurrTestClass, { workflowID: uuid }).testWorkflow();
    await ConcurrTestClass.promise2;
    ConcurrTestClass.resolve3();
    await handle1.getResult();

    ConcurrTestClass.resolve();
    await handle2.getResult();

    expect(ConcurrTestClass.cnt).toBe(1);
    expect(ConcurrTestClass.wfCnt).toBe(2);
  });

  test('duplicate-step', async () => {
    // Run two steps concurrently with the same UUID; both should succeed.
    // Since we only record the output after the function, it may cause more than once executions.
    const workflowUUID = uuidv1();
    const results = await Promise.allSettled([
      (await DBOS.startWorkflow(ConcurrTestClass, { workflowID: workflowUUID }).testStep(11)).getResult(),
      (await DBOS.startWorkflow(ConcurrTestClass, { workflowID: workflowUUID }).testStep(11)).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(11);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(11);

    expect(ConcurrTestClass.cnt).toBeGreaterThanOrEqual(1);
  });

  test('duplicate-notifications', async () => {
    // Run two send/recv concurrently with the same UUID, both should succeed.
    // It's a bit hard to trigger conflicting send because the transaction runs quickly.
    const recvUUID = uuidv1();
    const recvResPromise = Promise.allSettled([
      (
        await DBOS.startWorkflow(ConcurrTestClass, { workflowID: recvUUID }).receiveWorkflow('testTopic', 2)
      ).getResult(),
      (
        await DBOS.startWorkflow(ConcurrTestClass, { workflowID: recvUUID }).receiveWorkflow('testTopic', 2)
      ).getResult(),
    ]);

    // Send would trigger both to receive, but only one can succeed.
    await sleepms(10); // Both would be listening to the notification.

    await expect(DBOS.send(recvUUID, 'testmsg', 'testTopic')).resolves.toBeFalsy();

    const recvRes = await recvResPromise;
    expect((recvRes[0] as PromiseFulfilledResult<string | null>).value).toBe('testmsg');
    expect((recvRes[1] as PromiseFulfilledResult<string | null>).value).toBe('testmsg');

    const recvHandle = DBOS.retrieveWorkflow(recvUUID);
    await expect(recvHandle.getResult()).resolves.toBe('testmsg');
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

  static resolve3: () => void;
  static promise3 = new Promise<void>((r) => {
    ConcurrTestClass.resolve2 = r;
  });

  @DBOS.transaction()
  static async testReadWriteFunction(id: number) {
    await DBOS.pgClient.query(`INSERT INTO ${testTableName}(id, value) VALUES ($1, $2)`, [1, id]);
    ConcurrTestClass.cnt++;
    return id;
  }

  @DBOS.workflow()
  static async testWorkflow() {
    if (ConcurrTestClass.wfCnt++ === 1) {
      ConcurrTestClass.resolve2();
      await ConcurrTestClass.promise;
    } else {
      await ConcurrTestClass.promise3;
    }
    await ConcurrTestClass.testReadWriteFunction(1);
  }

  @DBOS.step()
  static async testStep(id: number) {
    ConcurrTestClass.cnt++;
    return Promise.resolve(id);
  }

  @DBOS.workflow()
  static async receiveWorkflow(topic: string, timeout: number) {
    return DBOS.recv<string>(topic, timeout);
  }
}
