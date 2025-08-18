import { DBOS } from '../src';
import { randomUUID } from 'node:crypto';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { KnexDataSource } from '../packages/knex-datasource';

const testTableName = 'dbos_concurrency_test_kv';

const config = generateDBOSTestConfig('pg-node');
const dbConfig = { client: 'pg', connection: { user: 'postgres', database: 'dbostest' } };
const knexds = new KnexDataSource('app-db', dbConfig);

const simpleWF = DBOS.registerWorkflow(
  async () => {
    return Promise.resolve('WF Ran');
  },
  { name: 'simpleWF' },
);

const runALotOfThingsAtOnce = DBOS.registerWorkflow(
  async (conc: boolean, baseid: number) => {
    const things: { func: () => Promise<string>; expected: string }[] = [
      {
        // Reads context into locals, OK
        func: async () => {
          await DBOS.sleepms(500);
          return 'slept';
        },
        expected: 'slept',
      },
      {
        func: async () => {
          return await DBOS.runStep(() => Promise.resolve('ranStep'), { name: 'runStep1' });
        },
        expected: 'ranStep',
      },
      {
        func: async () => {
          return await DBOS.runStep(() => Promise.resolve('ranStep'), { name: 'runStep2' });
        },
        expected: 'ranStep',
      },
      {
        func: async () => {
          return await DBOS.runStep(
            () => {
              if (DBOS.stepStatus!.currentAttempt! <= 2) throw new Error('Not yet');
              return Promise.resolve('ranStep');
            },
            { retriesAllowed: true, maxAttempts: 5, intervalSeconds: 0.1, backoffRate: 1, name: 'runStepRetry' },
          );
        },
        expected: 'ranStep',
      },
      /*
      {
        func: async () => {
          return (await ConcurrTestClass.testDSReadWrite(baseid + 1)).toString();
        },
        expected: `${baseid + 1}`,
      },
      {
        func: async () => {
          return (await ConcurrTestClass.testReadWriteFunction(baseid + 2)).toString();
        },
        expected: `${baseid + 2}`,
      },
      {
        func: async () => {
          return (
            await knexds.runTransaction(
              async () => {
                return Promise.resolve('A');
              },
              { name: 'runTx' },
            )
          ).toString();
        },
        expected: `A`,
      },
      */
      {
        func: async () => {
          return (await DBOS.now()).toString()[0];
        },
        expected: `1`,
      },
      {
        func: async () => {
          return (await DBOS.randomUUID()).length.toString();
        },
        expected: `36`,
      },
      /*
      {
        func: async () => {
          await DBOS.setEvent('eventkey', 'eval');
          return 'set';
        },
        expected: `set`,
      },
      {
        func: async () => {
          return (await DBOS.getEvent(DBOS.workflowID!, 'eventkey')) ?? 'Nope';
        },
        expected: `eval`,
      },
      {
        func: async () => {
          await DBOS.send(DBOS.workflowID!, 'msg', 'topic');
          return 'sent';
        },
        expected: `sent`,
      },
      {
        func: async () => {
          return (await DBOS.recv('topic')) ?? 'Nope';
        },
        expected: `msg`,
      },
      {
        func: async () => {
          return (await DBOS.getWorkflowStatus('nosuchwv'))?.status ?? 'Nope';
        },
        expected: `Nope`,
      },
      {
        func: async () => {
          return await simpleWF();
        },
        expected: 'WF Ran',
      },
      {
        func: async () => {
          await DBOS.startWorkflow(simpleWF, { workflowID: `${DBOS.workflowID}-cwf` })();
          return 'started';
        },
        expected: 'started',
      },
      {
        func: async () => {
          const wfh = DBOS.retrieveWorkflow<string>(`${DBOS.workflowID}-cwf`);
          return await wfh.getResult();
        },
        expected: 'WF Ran',
      },
      {
        func: async () => {
          await DBOS.writeStream('stream', 'val');
          return 'wrote';
        },
        expected: 'wrote',
      },
      {
        func: async () => {
          const { value } = await DBOS.readStream(DBOS.workflowID!, 'stream').next();
          return value as string;
        },
        expected: 'val',
      },
      */
    ];

    if (conc) {
      const promises: Promise<string>[] = [];
      for (const t of things) {
        promises.push(t.func());
      }
      const res = await Promise.allSettled(promises);
      for (let i = 0; i < things.length; ++i) {
        expect((res[i] as PromiseFulfilledResult<string>).value).toBe(things[i].expected);
      }
    } else {
      for (const t of things) {
        const res = await t.func();
        expect(res).toBe(t.expected);
      }
    }
  },
  { name: 'runALotOfThingsAtOnce' },
);

describe('concurrency-tests', () => {
  beforeAll(async () => {
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
    const workflowUUID = randomUUID();
    const results = await Promise.allSettled([
      (await DBOS.startWorkflow(ConcurrTestClass, { workflowID: workflowUUID }).testReadWriteFunction(10)).getResult(),
      (await DBOS.startWorkflow(ConcurrTestClass, { workflowID: workflowUUID }).testReadWriteFunction(10)).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(10);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(10);
    expect(ConcurrTestClass.cnt).toBe(1);
  });

  test('duplicate-dstransaction', async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workflowUUID = randomUUID();
    const results = await Promise.allSettled([
      (
        await DBOS.startWorkflow(ConcurrTestClass, { workflowID: workflowUUID }).testDSReadWriteFunction(10)
      ).getResult(),
      (
        await DBOS.startWorkflow(ConcurrTestClass, { workflowID: workflowUUID }).testDSReadWriteFunction(10)
      ).getResult(),
    ]);
    expect((results[0] as PromiseFulfilledResult<number>).value).toBe(10);
    expect((results[1] as PromiseFulfilledResult<number>).value).toBe(10);
    expect(ConcurrTestClass.cnt).toBe(1);
  });

  test('concurrent-workflow', async () => {
    // Invoke testWorkflow twice with the same UUID and the first one completes right before the second transaction starts.
    // The second transaction should get the correct recorded execution without being executed.
    const uuid = randomUUID();
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
    const workflowUUID = randomUUID();
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
    const recvUUID = randomUUID();
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

  test('promise-all-settled', async () => {
    const wfidSerial = randomUUID();
    const wfidConcurrent = randomUUID();

    await DBOS.withNextWorkflowID(wfidSerial, async () => {
      await runALotOfThingsAtOnce(false, 100);
    });
    await DBOS.withNextWorkflowID(wfidConcurrent, async () => {
      await runALotOfThingsAtOnce(true, 200);
    });

    const wfstepsSerial = (await DBOS.listWorkflowSteps(wfidSerial))!;
    const wfstepsConcurrent = (await DBOS.listWorkflowSteps(wfidConcurrent))!;

    for (let i = 0; i < wfstepsSerial.length; ++i) {
      console.log(`Output of ${wfstepsConcurrent[i].name}: ${JSON.stringify(wfstepsConcurrent[i].output)}`);
      expect(wfstepsConcurrent[i].name).toBe(wfstepsSerial[i].name);
      expect(wfstepsConcurrent[i].functionID).toBe(wfstepsSerial[i].functionID);
      expect(wfstepsConcurrent[i].error).toStrictEqual(wfstepsSerial[i].error);
      if (['DBOS.now', 'DBOS.randomUUID', 'DBOS.sleep'].includes(wfstepsConcurrent[i].name)) continue;
      if (['testDSReadWrite', 'testReadWriteFunction'].includes(wfstepsConcurrent[i].name)) {
        // We use a different ID, the bottom digit matters
        expect((wfstepsConcurrent[i].output as string)[2]).toStrictEqual((wfstepsSerial[i].output as string)[2]);
        continue;
      }
      expect(wfstepsConcurrent[i].output).toStrictEqual(wfstepsSerial[i].output);
    }

    await runALotOfThingsAtOnce(true, 300);
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
    ConcurrTestClass.resolve3 = r;
  });

  @DBOS.transaction()
  static async testReadWriteFunction(id: number) {
    await DBOS.pgClient.query(`INSERT INTO ${testTableName}(id, value) VALUES ($1, $2)`, [id, 1]);
    ConcurrTestClass.cnt++;
    return id;
  }

  @DBOS.workflow()
  static async testDSReadWriteFunction(id: number) {
    return await ConcurrTestClass.testDSReadWrite(id);
  }

  @knexds.transaction()
  static async testDSReadWrite(id: number) {
    await knexds.client.raw(`INSERT INTO ${testTableName}(id, value) VALUES (?, ?)`, [id, 1]);
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
