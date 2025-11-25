import { DBOS, DBOSConfig } from '../src';
import { randomUUID } from 'node:crypto';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { KnexDataSource } from '../packages/knex-datasource';
import { StepInfo } from '../src/workflow';
import { ensurePGDatabase } from '../src/database_utils';

const config = generateDBOSTestConfig();

const dbname = 'conc_test_userdb';
const dbConfig = { user: process.env.PGUSER || 'postgres', database: dbname };
const knexConfig = { client: 'pg', connection: dbConfig };
const knexds = new KnexDataSource('app-db', knexConfig);

const testTableName = 'dbos_concurrency_test_kv';

const simpleWF = DBOS.registerWorkflow(
  async () => {
    return Promise.resolve('WF Ran');
  },
  { name: 'simpleWF' },
);

const regStep = DBOS.registerStep(
  async () => {
    return Promise.resolve('regStep ran');
  },
  { name: 'regStep' },
);

const regStepRetry = DBOS.registerStep(
  async () => {
    if (DBOS.stepStatus!.currentAttempt! <= 2) throw new Error('Not yet');
    return Promise.resolve('regStepRetry ran');
  },
  { name: 'regStepRetry', retriesAllowed: true, maxAttempts: 5, intervalSeconds: 0.01, backoffRate: 1 },
);

const cleanDB = knexds.registerTransaction(async () => {
  await knexds.client.raw(`DELETE FROM ${testTableName}`);
});

const regDSTx = knexds.registerTransaction(
  async (id) => {
    await knexds.client.raw(`INSERT INTO ${testTableName}(id, value) VALUES (?, ?)`, [id, 1]);
    return 'tx done';
  },
  { name: 'regDSTx' },
);

let retryCnt = 0;
const regDSTxRetry = knexds.registerTransaction(
  async (id) => {
    if (retryCnt <= 2) {
      ++retryCnt;
      const e = new Error('Not yet') as Error & { code: string };
      e.code = '40001';
      throw e;
    }
    retryCnt = 0;
    await knexds.client.raw(`INSERT INTO ${testTableName}(id, value) VALUES (?, ?)`, [id, 1]);
    return 'txr done';
  },
  { name: 'regDSTxRetry' },
);

const runALotOfThingsAtOnce = DBOS.registerWorkflow(
  async (conc: boolean) => {
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
        // Step clones the ctx
        func: async () => {
          return await DBOS.runStep(() => Promise.resolve('ranStep'), { name: 'runStep1' });
        },
        expected: 'ranStep',
      },
      {
        // Step clones the ctx
        func: async () => {
          return await DBOS.runStep(() => Promise.resolve('ranStep'), { name: 'runStep2' });
        },
        expected: 'ranStep',
      },
      {
        // Step clones the ctx
        func: async () => {
          return await DBOS.runStep(
            () => {
              if (DBOS.stepStatus!.currentAttempt! <= 2) throw new Error('Not yet');
              return Promise.resolve('ranStep');
            },
            { retriesAllowed: true, maxAttempts: 5, intervalSeconds: 0.01, backoffRate: 1, name: 'runStepRetry' },
          );
        },
        expected: 'ranStep',
      },
      {
        // Tx clones the ctx
        func: async () => {
          return (await ConcurrTestClass.testReadWriteFunction(2)).toString();
        },
        expected: `2`,
      },
      {
        func: async () => {
          return (await ConcurrTestClass.testDSReadWrite(1)).toString();
        },
        expected: `1`,
      },
      {
        func: regStep,
        expected: 'regStep ran',
      },
      {
        // Grabs the call num before running in internalStep
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
      {
        func: async () => {
          return (await DBOS.now()).toString()[0];
        },
        expected: `1`,
      },
      {
        func: async () => {
          return regDSTx(7);
        },
        expected: 'tx done',
      },
      {
        func: async () => {
          return (await DBOS.randomUUID()).length.toString();
        },
        expected: `36`,
      },
      {
        // These get func IDs and pass directly to sysdb
        func: async () => {
          await DBOS.setEvent('eventkey', 'eval');
          return 'set';
        },
        expected: `set`,
      },
      {
        func: regStepRetry,
        expected: 'regStepRetry ran',
      },
      {
        func: async () => {
          return (await DBOS.getEvent(DBOS.workflowID!, 'eventkey')) ?? 'Nope';
        },
        expected: `eval`,
      },
      {
        // These get func IDs and pass directly to sysdb
        func: async () => {
          await DBOS.send(DBOS.workflowID!, 'msg', 'topic');
          return 'sent';
        },
        expected: `sent`,
      },
      {
        func: () => ConcurrTestClass.testStepStr('3'),
        expected: '3',
      },
      {
        func: async () => {
          return regDSTxRetry(8);
        },
        expected: 'txr done',
      },
      {
        func: async () => {
          return (await DBOS.recv('topic')) ?? 'Nope';
        },
        expected: `msg`,
      },
      {
        // Gets func ID in advance of sysdb call
        func: async () => {
          return (await DBOS.getWorkflowStatus('nosuchwv'))?.status ?? 'Nope';
        },
        expected: `Nope`,
      },
      {
        // Needs to get its start and getresult func IDs in advance
        func: async () => {
          return await simpleWF();
        },
        expected: 'WF Ran',
      },
      {
        func: () => ConcurrTestClass.testStepRetry('4'),
        expected: '4',
      },
      {
        // Gets start func ID in advance
        func: async () => {
          await DBOS.startWorkflow(simpleWF, { workflowID: `${DBOS.workflowID}-cwf` })();
          return 'started';
        },
        expected: 'started',
      },
      {
        // getResult the func ID for the handle
        func: async () => {
          const wfh = DBOS.retrieveWorkflow<string>(`${DBOS.workflowID}-cwf`);
          return await wfh.getResult();
        },
        expected: 'WF Ran',
      },
      {
        // Uses sysdb directly after getting func id
        func: async () => {
          await DBOS.writeStream('stream', 'val');
          return 'wrote';
        },
        expected: 'wrote',
      },
      {
        // Uses sysdb directly (polls, does not WF checkpoint)
        func: async () => {
          const { value } = await DBOS.readStream(DBOS.workflowID!, 'stream').next();
          return value as string;
        },
        expected: 'val',
      },
    ];

    await runThingsSerialOrConc(conc, things);
  },
  { name: 'runALotOfThingsAtOnce' },
);

const runALotOfStepsAtOnce = DBOS.registerWorkflow(
  async (conc: boolean) => {
    const things: { func: () => Promise<string>; expected: string }[] = [
      {
        // Step clones the ctx
        func: async () => {
          return await DBOS.runStep(() => Promise.resolve('ranStep'), { name: 'runStep1a' });
        },
        expected: 'ranStep',
      },
      {
        // Step clones the ctx
        func: async () => {
          return await DBOS.runStep(
            () => {
              if (DBOS.stepStatus!.currentAttempt! <= 2) throw new Error('Not yet');
              return Promise.resolve('ranStep');
            },
            { retriesAllowed: true, maxAttempts: 5, intervalSeconds: 0.02, backoffRate: 1, name: 'runStepRetry1' },
          );
        },
        expected: 'ranStep',
      },
      {
        func: regStepRetry,
        expected: 'regStepRetry ran',
      },
      {
        // Step clones the ctx
        func: async () => {
          return await DBOS.runStep(
            () => {
              if (DBOS.stepStatus!.currentAttempt! <= 2) throw new Error('Not yet');
              return Promise.resolve('ranStep');
            },
            { retriesAllowed: true, maxAttempts: 5, intervalSeconds: 0.01, backoffRate: 1, name: 'runStepRetry2' },
          );
        },
        expected: 'ranStep',
      },
      {
        // Step clones the ctx
        func: async () => {
          return await DBOS.runStep(() => Promise.resolve('ranStep'), { name: 'runStep2' });
        },
        expected: 'ranStep',
      },
      {
        func: () => ConcurrTestClass.testStepStr('3'),
        expected: '3',
      },
      {
        func: regStep,
        expected: 'regStep ran',
      },
      {
        // Step clones the ctx
        func: async () => {
          return await DBOS.runStep(
            () => {
              if (DBOS.stepStatus!.currentAttempt! <= 2) throw new Error('Not yet');
              return Promise.resolve('ranStep');
            },
            { retriesAllowed: true, maxAttempts: 5, intervalSeconds: 0.03, backoffRate: 1, name: 'runStepRetry3' },
          );
        },
        expected: 'ranStep',
      },
      {
        func: () => ConcurrTestClass.testStepRetry('4'),
        expected: '4',
      },
    ];

    await runThingsSerialOrConc(conc, things);
  },
  { name: 'runALotOfStepsAtOnce' },
);

const runALotOfTransactionsAtOnce = DBOS.registerWorkflow(
  async (conc: boolean) => {
    const things: { func: () => Promise<string>; expected: string }[] = [
      {
        func: async () => {
          return (await ConcurrTestClass.testReadWriteFunction(2)).toString();
        },
        expected: `2`,
      },
      {
        func: async () => {
          return (await ConcurrTestClass.testDSReadWrite(1)).toString();
        },
        expected: `1`,
      },
      {
        func: async () => {
          return regDSTx(7);
        },
        expected: 'tx done',
      },
      {
        func: async () => {
          return (await ConcurrTestClass.testReadWriteFunction(4)).toString();
        },
        expected: `4`,
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
      {
        func: async () => {
          return (await ConcurrTestClass.testDSReadWrite(3)).toString();
        },
        expected: `3`,
      },
      {
        func: async () => {
          return regDSTxRetry(8);
        },
        expected: 'txr done',
      },
      {
        func: async () => {
          return (
            await knexds.runTransaction(
              async () => {
                await knexds.client.raw(`INSERT INTO ${testTableName}(id, value) VALUES (?, ?)`, [5, 1]);
                return Promise.resolve('5');
              },
              { name: 'runTx5' },
            )
          ).toString();
        },
        expected: `5`,
      },
    ];

    await runThingsSerialOrConc(conc, things);
  },
  { name: 'runALotOfTransactionsAtOnce' },
);

describe('concurrency-tests', () => {
  beforeAll(async () => {
    await ensurePGDatabase({
      dbToEnsure: dbname,
      adminUrl: `postgresql://${dbConfig.user}:${process.env['PGPASSWORD'] || 'dbos'}@${process.env['PGHOST'] || 'localhost'}:${process.env['PGPORT'] || '5432'}/postgres`,
    });

    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();

    await knexds.runTransaction(async () => {
      await knexds.client.raw(`DROP TABLE IF EXISTS ${testTableName};`);
      await knexds.client.raw(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`);
    });

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

  test('promise-all-settled-manythings', async () => {
    const wfidSerial = randomUUID();
    const wfidConcurrent = randomUUID();

    await cleanDB();
    await DBOS.withNextWorkflowID(wfidSerial, async () => {
      await runALotOfThingsAtOnce(false);
    });
    await cleanDB();
    await DBOS.withNextWorkflowID(wfidConcurrent, async () => {
      await runALotOfThingsAtOnce(true);
    });

    const wfstepsSerial = (await DBOS.listWorkflowSteps(wfidSerial))!;
    const wfstepsConcurrent = (await DBOS.listWorkflowSteps(wfidConcurrent))!;

    compareWFRuns(wfstepsSerial, wfstepsConcurrent);

    await cleanDB();
    await runALotOfThingsAtOnce(true);
  }, 30000);

  test('promise-all-settled-manysteps', async () => {
    const wfidSerial = randomUUID();
    const wfidConcurrent = randomUUID();

    await DBOS.withNextWorkflowID(wfidSerial, async () => {
      await runALotOfStepsAtOnce(false);
    });
    await DBOS.withNextWorkflowID(wfidConcurrent, async () => {
      await runALotOfStepsAtOnce(true);
    });

    const wfstepsSerial = (await DBOS.listWorkflowSteps(wfidSerial))!;
    const wfstepsConcurrent = (await DBOS.listWorkflowSteps(wfidConcurrent))!;

    compareWFRuns(wfstepsSerial, wfstepsConcurrent);

    const fwf1 = await DBOS.forkWorkflow(wfidConcurrent, 3);
    await fwf1.getResult();
    const fwf2 = await DBOS.forkWorkflow(wfidConcurrent, 5);
    await fwf2.getResult();
    const fwf3 = await DBOS.forkWorkflow(wfidConcurrent, 7);
    await fwf3.getResult();
  }, 20000);

  test('promise-all-settled-transactions', async () => {
    const wfidSerial = randomUUID();
    const wfidConcurrent = randomUUID();

    await cleanDB();
    await DBOS.withNextWorkflowID(wfidSerial, async () => {
      await runALotOfTransactionsAtOnce(false);
    });
    await cleanDB();
    await DBOS.withNextWorkflowID(wfidConcurrent, async () => {
      await runALotOfTransactionsAtOnce(true);
    });

    const wfstepsSerial = (await DBOS.listWorkflowSteps(wfidSerial))!;
    const wfstepsConcurrent = (await DBOS.listWorkflowSteps(wfidConcurrent))!;

    compareWFRuns(wfstepsSerial, wfstepsConcurrent);

    await cleanDB();
    const fwf1 = await DBOS.forkWorkflow(wfidConcurrent, 2);
    await fwf1.getResult();
    await cleanDB();
    const fwf2 = await DBOS.forkWorkflow(wfidConcurrent, 5);
    await fwf2.getResult();
    await cleanDB();
    const fwf3 = await DBOS.forkWorkflow(wfidConcurrent, 7);
    await fwf3.getResult();
  }, 20000);
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

  @DBOS.step()
  static async testReadWriteFunction(id: number) {
    ConcurrTestClass.cnt++;
    return Promise.resolve(id);
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

  @DBOS.step()
  static async testStep(id: number) {
    ConcurrTestClass.cnt++;
    return Promise.resolve(id);
  }

  @DBOS.step()
  static async testStepStr(id: string) {
    return Promise.resolve(id);
  }
  @DBOS.step({ retriesAllowed: true, maxAttempts: 5, intervalSeconds: 0.01, backoffRate: 1 })
  static async testStepRetry(id: string) {
    if (DBOS.stepStatus!.currentAttempt! <= 2) throw new Error('Not yet');
    return Promise.resolve(id);
  }

  @DBOS.workflow()
  static async receiveWorkflow(topic: string, timeout: number) {
    return DBOS.recv<string>(topic, timeout);
  }
}
function compareWFRuns(wfstepsSerial: StepInfo[], wfstepsConcurrent: StepInfo[]) {
  let iconc = 0;
  for (let i = 0; i < wfstepsSerial.length; ) {
    console.log(
      `Output of ${wfstepsConcurrent[iconc].name}@${wfstepsConcurrent[iconc].functionID}: ${JSON.stringify(wfstepsConcurrent[iconc].output)} vs. ${wfstepsSerial[i].name}@${wfstepsSerial[i].functionID}: ${JSON.stringify(wfstepsSerial[i].output)}`,
    );
    // Sleeps in things like getResult may or may not appear in parallel execution
    //   (serial must do the set first and will have no sleep).
    // They will be assigned consistent IDs in both cases, at least.
    if (
      wfstepsConcurrent[iconc].functionID < wfstepsSerial[i].functionID &&
      wfstepsConcurrent[iconc].name === 'DBOS.sleep'
    ) {
      ++iconc;
      continue;
    }
    expect(wfstepsConcurrent[iconc].functionID).toBe(wfstepsSerial[i].functionID);
    expect(wfstepsConcurrent[iconc].name).toBe(wfstepsSerial[i].name);
    expect(wfstepsConcurrent[iconc].error).toStrictEqual(wfstepsSerial[i].error);
    if (['DBOS.now', 'DBOS.randomUUID', 'DBOS.sleep'].includes(wfstepsConcurrent[iconc].name)) {
      // Result may differ, that's all
    } else {
      expect(wfstepsConcurrent[iconc].output).toStrictEqual(wfstepsSerial[i].output);
    }
    ++i;
    ++iconc;
  }
}

async function runThingsSerialOrConc(conc: boolean, things: { func: () => Promise<string>; expected: string }[]) {
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
}

describe('concurrent-events', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('concurrent-workflow-events', async () => {
    const wfid = randomUUID();
    const promises: Promise<string | null>[] = [];
    for (let i = 0; i < 10; ++i) {
      promises.push(DBOS.getEvent(wfid, 'key1'));
    }

    const st = Date.now();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await DBOSTestClassWFS.setEventWorkflow();
    });
    const et = Date.now();
    expect(et - st).toBeLessThan(1000);

    await Promise.allSettled(promises);
  });
});

class DBOSTestClassWFS {
  @DBOS.workflow()
  static async setEventWorkflow() {
    await DBOS.setEvent('key1', 'value1');
    return 0;
  }
}
