import { PoolClient } from 'pg';
import {
  DBOS,
  Scheduled,
  SchedulerMode,
  Transaction,
  TransactionContext,
  Workflow,
  WorkflowContext,
  WorkflowQueue,
} from '../../src';
import { DBOSConfig } from '../../src/dbos-executor';
import { sleepms } from '../../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';

type TestTransactionContext = TransactionContext<PoolClient>;

describe('scheduled-wf-tests-simple', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    DBOSSchedTestClass.reset(true);
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.dropSystemDB();
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  test('wf-scheduled', async () => {
    // Make sure two functions with the same name in different classes are not interfering with each other.
    await sleepms(2500);
    expect(DBOSSchedDuplicate.nCalls).toBeGreaterThanOrEqual(2);
    expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2);
    expect(DBOSSchedTestClass.nTooEarly).toBe(0);
    expect(DBOSSchedTestClass.nTooLate).toBe(0);

    await DBOS.deactivateEventReceivers();

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

  @Transaction({ isolationLevel: 'READ COMMITTED' })
  static async scheduledTxn(ctxt: TestTransactionContext) {
    DBOSSchedTestClass.nCalls++;
    await ctxt.client.query('SELECT 1');
  }

  @Scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive })
  @Workflow()
  static async scheduledDefault(ctxt: WorkflowContext, schedTime: Date, startTime: Date) {
    await ctxt.invoke(DBOSSchedTestClass).scheduledTxn();
    if (schedTime.getTime() > startTime.getTime() + 2) {
      // Floating point, sleep, etc., is a little imprecise
      console.log(
        `Scheduled 'scheduledDefault' function running early: ${ctxt.workflowUUID}; at ${startTime.toISOString()} vs ${schedTime.toISOString()}`,
      );
      DBOSSchedTestClass.nTooEarly++;
    }
    if (startTime.getTime() - schedTime.getTime() > 1500) DBOSSchedTestClass.nTooLate++;

    if (DBOSSchedTestClass.doSleep) {
      await ctxt.sleepms(2000);
    }
  }

  @Scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive, queueName: q.name })
  @Workflow()
  static async scheduledDefaultQ(_ctxt: WorkflowContext, _schedTime: Date, _startTime: Date) {
    ++DBOSSchedTestClass.nQCalls;
    return Promise.resolve();
  }

  // This should run every 30 minutes. Making sure the testing runtime can correctly exit within a reasonable time.
  @Scheduled({ crontab: '*/30 * * * *' })
  @Workflow()
  static async scheduledLong(ctxt: WorkflowContext, _schedTime: Date, _startTime: Date) {
    await ctxt.sleepms(100);
  }
}

class DBOSSchedDuplicate {
  static nCalls = 0;

  @Transaction({ isolationLevel: 'READ COMMITTED' })
  static async scheduledTxn(ctxt: TestTransactionContext) {
    DBOSSchedDuplicate.nCalls++;
    await ctxt.client.query('SELECT 1');
  }

  @Scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive })
  @Workflow()
  static async scheduledDefault(ctxt: WorkflowContext, _schedTime: Date, _startTime: Date) {
    await ctxt.invoke(DBOSSchedDuplicate).scheduledTxn();
    return Promise.resolve();
  }
}

class DBOSSchedTestClassOAOO {
  static nCalls = 0;

  static reset() {
    DBOSSchedTestClassOAOO.nCalls = 0;
  }

  @Scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerInterval })
  @Workflow()
  static async scheduledDefault(_ctxt: WorkflowContext, _schedTime: Date, _startTime: Date) {
    DBOSSchedTestClassOAOO.nCalls++;
    return Promise.resolve();
  }
}

describe('scheduled-wf-tests-oaoo', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {});

  afterEach(async () => {}, 10000);

  test('wf-scheduled-recover', async () => {
    DBOSSchedTestClassOAOO.reset();
    await DBOS.launch();
    let startDownTime: number[] = [];
    try {
      await sleepms(3000);
      expect(DBOSSchedTestClassOAOO.nCalls).toBeGreaterThanOrEqual(2);
      expect(DBOSSchedTestClassOAOO.nCalls).toBeLessThanOrEqual(4);
      startDownTime = process.hrtime();
    } finally {
      await DBOS.shutdown();
    }
    await sleepms(5000);
    DBOSSchedTestClassOAOO.reset();
    await DBOS.launch();
    try {
      await sleepms(3000);
      expect(DBOSSchedTestClassOAOO.nCalls).toBeGreaterThanOrEqual(7); // 3 new ones, 5 recovered ones, +/-
      const endDownTime = process.hrtime();
      const nShouldHaveHappened = endDownTime[0] - startDownTime[0] + 2;
      expect(DBOSSchedTestClassOAOO.nCalls).toBeLessThanOrEqual(nShouldHaveHappened);
    } finally {
      await DBOS.shutdown();
    }
  }, 20000);
});

describe('scheduled-wf-tests-when-active', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {});

  afterEach(async () => {}, 10000);

  test('wf-scheduled-recover', async () => {
    DBOSSchedTestClass.reset(false);
    await DBOS.launch();
    try {
      await sleepms(3000);
      expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2);
      expect(DBOSSchedTestClass.nCalls).toBeLessThanOrEqual(4);
      expect(DBOSSchedTestClass.nQCalls).toBeGreaterThanOrEqual(1); // This has some delay, potentially...

      const wfs = await DBOS.listWorkflows({
        workflowName: 'scheduledDefaultQ',
      });

      let foundQ = false;
      for (const wf of wfs) {
        const stat = await DBOS.retrieveWorkflow(wf.workflowID).getStatus();
        if (stat?.queueName === q.name) foundQ = true;
      }
      expect(foundQ).toBeTruthy();
    } finally {
      await DBOS.shutdown();
    }
    await sleepms(5000);
    DBOSSchedTestClass.reset(false);
    await DBOS.launch();
    try {
      await sleepms(3000);
      expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2); // 3 new ones, +/- 1
      expect(DBOSSchedTestClass.nCalls).toBeLessThanOrEqual(4); // No old ones from recovery or sleep interval
    } finally {
      await DBOS.shutdown();
    }
  }, 15000);
});
