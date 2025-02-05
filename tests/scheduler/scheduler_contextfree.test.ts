import { DBOS, SchedulerMode, TestingRuntime, WorkflowQueue } from '../../src';
import { DBOSConfig } from '../../src/dbos-executor';
import { createInternalTestRuntime } from '../../src/testing/testing_runtime';
import { sleepms } from '../../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';

describe('cf-scheduled-wf-tests-simple', () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    DBOSSchedTestClass.reset(true);
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime(undefined, config);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  }, 10000);

  test('wf-scheduled', async () => {
    // Make sure two functions with the same name in different classes are not interfering with each other.
    await sleepms(2500);
    expect(DBOSSchedDuplicate.nCalls).toBeGreaterThanOrEqual(2);
    expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2);
    expect(DBOSSchedTestClass.nTooEarly).toBe(0);
    expect(DBOSSchedTestClass.nTooLate).toBe(0);

    await testRuntime.deactivateEventReceivers();

    await sleepms(1000);
    expect(DBOSSchedTestClass.nCalls).toBeLessThanOrEqual(3);
  });
});

const q = new WorkflowQueue('schedQ', 1);

class DBOSSchedTestClass {
  static nCalls = 0;
  static nQCalls = 0;
  static nTooEarly = 0;
  static nTooLate = 0;
  static doSleep = true;

  static reset(doSleep: boolean) {
    DBOSSchedTestClass.nCalls = 0;
    DBOSSchedTestClass.nQCalls = 0;
    DBOSSchedTestClass.nTooEarly = 0;
    DBOSSchedTestClass.nTooLate = 0;
    DBOSSchedTestClass.doSleep = doSleep;
  }

  @DBOS.transaction({ isolationLevel: 'READ COMMITTED' })
  static async scheduledTxn() {
    DBOSSchedTestClass.nCalls++;
    await DBOS.pgClient.query('SELECT 1');
  }

  @DBOS.scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive })
  @DBOS.workflow()
  static async scheduledDefault(schedTime: Date, startTime: Date) {
    await DBOSSchedTestClass.scheduledTxn();
    if (schedTime.getTime() > startTime.getTime() + 0.002) {
      // Floating point, sleep, etc., is a little imprecise
      DBOS.logger.warn(
        `Scheduled 'scheduledDefault' function running early: ${DBOS.workflowID}; at ${startTime.toISOString()} vs ${schedTime.toISOString()}`,
      );
      DBOSSchedTestClass.nTooEarly++;
    }
    if (startTime.getTime() - schedTime.getTime() > 1500) DBOSSchedTestClass.nTooLate++;

    if (DBOSSchedTestClass.doSleep) {
      await DBOS.sleepms(2000);
    }
  }

  @DBOS.scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive, queueName: q.name })
  @DBOS.workflow()
  static async scheduledDefaultQ(_schedTime: Date, _startTime: Date) {
    ++DBOSSchedTestClass.nQCalls;
    return Promise.resolve();
  }

  // This should run every 30 minutes. Making sure the testing runtime can correctly exit within a reasonable time.
  @DBOS.scheduled({ crontab: '*/30 * * * *' })
  @DBOS.workflow()
  static async scheduledLong(_schedTime: Date, _startTime: Date) {
    await DBOS.sleepms(100);
  }
}

class DBOSSchedDuplicate {
  static nCalls = 0;

  @DBOS.transaction({ isolationLevel: 'READ COMMITTED' })
  static async scheduledTxn() {
    DBOSSchedDuplicate.nCalls++;
    await DBOS.pgClient.query('SELECT 1');
  }

  @DBOS.scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive })
  @DBOS.workflow()
  static async scheduledDefault(_schedTime: Date, _startTime: Date) {
    await DBOSSchedDuplicate.scheduledTxn();
  }
}

class DBOSSchedTestClassOAOO {
  static nCalls = 0;

  static reset() {
    DBOSSchedTestClassOAOO.nCalls = 0;
  }

  @DBOS.scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerInterval })
  @DBOS.workflow()
  static async scheduledDefault(_schedTime: Date, _startTime: Date) {
    DBOSSchedTestClassOAOO.nCalls++;
    return Promise.resolve();
  }
}

describe('cf-scheduled-wf-tests-oaoo', () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {});

  afterEach(async () => {}, 10000);

  test('wf-scheduled-recover', async () => {
    DBOSSchedTestClassOAOO.reset();
    testRuntime = await createInternalTestRuntime(undefined, config);
    let startDownTime: number[] = [];
    try {
      await sleepms(3000);
      expect(DBOSSchedTestClassOAOO.nCalls).toBeGreaterThanOrEqual(2);
      expect(DBOSSchedTestClassOAOO.nCalls).toBeLessThanOrEqual(4);
      startDownTime = process.hrtime();
    } finally {
      await testRuntime.destroy();
    }
    await sleepms(5000);
    DBOSSchedTestClassOAOO.reset();
    testRuntime = await createInternalTestRuntime(undefined, config);
    try {
      await sleepms(3000);
      expect(DBOSSchedTestClassOAOO.nCalls).toBeGreaterThanOrEqual(7); // 3 new ones, 5 recovered ones, +/-
      const endDownTime = process.hrtime();
      const nShouldHaveHappened = endDownTime[0] - startDownTime[0] + 2;
      expect(DBOSSchedTestClassOAOO.nCalls).toBeLessThanOrEqual(nShouldHaveHappened);
    } finally {
      await testRuntime.destroy();
    }
  }, 15000);
});

describe('cf-scheduled-wf-tests-when-active', () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {});

  afterEach(async () => {}, 10000);

  test('wf-scheduled-recover', async () => {
    DBOSSchedTestClass.reset(false);
    testRuntime = await createInternalTestRuntime(undefined, config);
    try {
      await sleepms(3000);
      expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2);
      expect(DBOSSchedTestClass.nCalls).toBeLessThanOrEqual(4);
      expect(DBOSSchedTestClass.nQCalls).toBeGreaterThanOrEqual(1); // This has some delay, potentially...

      const wfs = await testRuntime.getWorkflows({
        workflowName: 'scheduledDefaultQ',
      });

      let foundQ = false;
      for (const wfid of wfs.workflowUUIDs) {
        const stat = await testRuntime.retrieveWorkflow(wfid).getStatus();
        if (stat?.queueName === q.name) foundQ = true;
      }
      expect(foundQ).toBeTruthy();
    } finally {
      await testRuntime.destroy();
    }
    await sleepms(5000);
    DBOSSchedTestClass.reset(false);
    testRuntime = await createInternalTestRuntime(undefined, config);
    try {
      await sleepms(3000);
      expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2); // 3 new ones, +/- 1
      expect(DBOSSchedTestClass.nCalls).toBeLessThanOrEqual(4); // No old ones from recovery or sleep interval
    } finally {
      await testRuntime.destroy();
    }
  }, 15000);
});
