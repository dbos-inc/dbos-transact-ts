import { StatusString, WorkflowHandle, DBOS, ConfiguredInstance, InitContext } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb, Event, queueEntriesAreCleanedUp } from './helpers';
import { WorkflowQueue } from '../src';
import { v4 as uuidv4 } from 'uuid';
import { sleepms } from '../src/utils';

const queue = new WorkflowQueue('testQ');
const serialqueue = new WorkflowQueue('serialQ', 1);
const serialqueueLimited = new WorkflowQueue('serialQL', {
  concurrency: 1,
  rateLimit: { limitPerPeriod: 10, periodSec: 1 },
});
const childqueue = new WorkflowQueue('childQ', 3);
const workerConcurrencyQueue = new WorkflowQueue('workerQ', { workerConcurrency: 1 });

const qlimit = 5;
const qperiod = 2;
const rlqueue = new WorkflowQueue('limited_queue', undefined, { limitPerPeriod: qlimit, periodSec: qperiod });

class InstanceStepTx extends ConfiguredInstance {
  constructor() {
    super('Instance');
  }

  initialize(_ctx: InitContext): Promise<void> {
    return Promise.resolve();
  }

  @DBOS.step()
  async testStep(arg: string): Promise<void> {
    expect(arg).toBe('a');
    ++InstanceStepTx.stepCnt;
    return Promise.resolve();
  }

  @DBOS.transaction()
  async testTx(arg: string): Promise<void> {
    expect(arg).toBe('a');
    ++InstanceStepTx.txCnt;
    return Promise.resolve();
  }

  static stepCnt = 0;
  static txCnt = 0;
  static reset() {
    InstanceStepTx.stepCnt = 0;
    InstanceStepTx.txCnt = 0;
  }
}

const inst = new InstanceStepTx();

class StaticStepTx extends ConfiguredInstance {
  constructor() {
    super('Instance');
  }

  initialize(_ctx: InitContext): Promise<void> {
    return Promise.resolve();
  }

  @DBOS.step()
  static async testStep(arg: string): Promise<void> {
    expect(arg).toBe('a');
    ++StaticStepTx.stepCnt;
    return Promise.resolve();
  }

  @DBOS.transaction()
  static async testTx(arg: string): Promise<void> {
    expect(arg).toBe('a');
    ++StaticStepTx.txCnt;
    return Promise.resolve();
  }

  static stepCnt = 0;
  static txCnt = 0;
  static reset() {
    StaticStepTx.stepCnt = 0;
    StaticStepTx.txCnt = 0;
  }
}

describe('queued-wf-tests-simple', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    StaticStepTx.reset();
    InstanceStepTx.reset();
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  test('run-step-tx', async () => {
    await StaticStepTx.testTx('a');
    await StaticStepTx.testStep('a');
    await inst.testTx('a');
    await inst.testStep('a');

    expect(StaticStepTx.stepCnt).toBe(1);
    expect(StaticStepTx.txCnt).toBe(1);
    expect(InstanceStepTx.stepCnt).toBe(1);
    expect(InstanceStepTx.txCnt).toBe(1);
  }, 10000);

  test('start-step-tx', async () => {
    await (await DBOS.startWorkflow(StaticStepTx).testTx('a')).getResult();
    await (await DBOS.startWorkflow(StaticStepTx).testStep('a')).getResult();
    await (await DBOS.startWorkflow(inst).testTx('a')).getResult();
    await (await DBOS.startWorkflow(inst).testStep('a')).getResult();

    expect(StaticStepTx.stepCnt).toBe(1);
    expect(StaticStepTx.txCnt).toBe(1);
    expect(InstanceStepTx.stepCnt).toBe(1);
    expect(InstanceStepTx.txCnt).toBe(1);
  }, 10000);
});
