import EventEmitter from 'node:events';
import { DBOS, SchedulerMode, WorkflowQueue } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { INTERNAL_QUEUE_NAME, sleepms } from '../src/utils';
import { dropDatabase, generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

describe('cf-scheduled-wf-tests-simple', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    DBOSSchedTestClass.reset(true);
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    expect(config.systemDatabaseUrl).toBeDefined();
    await dropDatabase(config.systemDatabaseUrl!);
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

const q = new WorkflowQueue('schedQ', { concurrency: 1 });

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

  @DBOS.step()
  static async scheduledStep() {
    DBOSSchedTestClass.nCalls++;
    await Promise.resolve();
  }

  @DBOS.scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive })
  @DBOS.workflow()
  static async scheduledDefault(schedTime: Date, startTime: Date) {
    await DBOSSchedTestClass.scheduledStep();
    if (schedTime.getTime() > startTime.getTime() + 2) {
      // Floating point, sleep, etc., is a little imprecise
      console.log(
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

  @DBOS.step()
  static async scheduledStep() {
    DBOSSchedDuplicate.nCalls++;
    await Promise.resolve();
  }

  @DBOS.scheduled({ crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive })
  @DBOS.workflow()
  static async scheduledDefault(_schedTime: Date, _startTime: Date) {
    await DBOSSchedDuplicate.scheduledStep();
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

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    expect(config.systemDatabaseUrl).toBeDefined();
    await dropDatabase(config.systemDatabaseUrl!);
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
  }, 15000);
});

describe('cf-scheduled-wf-tests-when-active', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
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

interface SchedulerEvents {
  scheduled: (functionName: string, schedTime: Date, startTime: Date, workflowID?: string) => void;
}

class SchedulerEmitter extends EventEmitter {
  override on<K extends keyof SchedulerEvents>(event: K, listener: SchedulerEvents[K]): this {
    return super.on(event, listener);
  }

  override emit<K extends keyof SchedulerEvents>(event: K, ...args: Parameters<SchedulerEvents[K]>): boolean {
    // DBOS.logger.info(`SchedulerEmitter topic ${args[1]} partition ${args[2]}`);
    return super.emit(event, ...args);
  }
}

const schedulerEmitter = new SchedulerEmitter();

async function scheduledFunc(schedTime: Date, startTime: Date) {
  schedulerEmitter.emit('scheduled', 'scheduledFunc', schedTime, startTime, DBOS.workflowID);
  await Promise.resolve();
}

const regScheduledFunc = DBOS.registerWorkflow(scheduledFunc);
DBOS.registerScheduled(regScheduledFunc, { crontab: '* * * * * *' });

export async function withTimeout<T>(promise: Promise<T>, ms: number, message = 'Timeout'): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout>;

  const timeout = new Promise<never>((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(message)), ms);
  });

  return await Promise.race([promise, timeout]).finally(() => clearTimeout(timeoutId));
}

describe('decorator-free-scheduled', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    DBOSSchedTestClass.reset(true);
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    expect(config.systemDatabaseUrl).toBeDefined();
    await dropDatabase(config.systemDatabaseUrl!);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  test('free-func-scheduled', async () => {
    const events: { functionName: string; schedTime: Date; startTime: Date; workflowID: string | undefined }[] = [];
    function onScheduled(functionName: string, schedTime: Date, startTime: Date, workflowID: string | undefined) {
      events.push({ functionName, schedTime, startTime, workflowID });
    }
    try {
      schedulerEmitter.on('scheduled', onScheduled);
      await sleepms(2500);
      expect(events.length).toBeGreaterThanOrEqual(2);
    } finally {
      schedulerEmitter.off('scheduled', onScheduled);
    }

    const workflows = await DBOS.listWorkflows({ workflowName: 'scheduledFunc' });
    expect(workflows.length).toBeGreaterThan(0);
    for (const workflow of workflows) {
      expect(workflow.queueName).toEqual(INTERNAL_QUEUE_NAME);
    }
  });
});
