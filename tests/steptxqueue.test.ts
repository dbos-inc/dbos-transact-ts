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
  async testStep(arg: string, rv?: string, id?: string): Promise<string> {
    expect(arg).toBe('a');
    if (id) {
      expect(DBOS.workflowID).toBe(id);
    }
    ++InstanceStepTx.stepCnt;
    return Promise.resolve(rv ?? '');
  }

  @DBOS.transaction()
  async testTx(arg: string, rv?: string, id?: string): Promise<string> {
    expect(arg).toBe('a');
    if (id) {
      expect(DBOS.workflowID).toBe(id);
    }
    ++InstanceStepTx.txCnt;
    return Promise.resolve(rv ?? '');
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
  static async testStep(arg: string, rv?: string, id?: string): Promise<string> {
    expect(arg).toBe('a');
    if (id) {
      expect(DBOS.workflowID).toBe(id);
    }
    ++StaticStepTx.stepCnt;
    return Promise.resolve(rv ?? '');
  }

  @DBOS.transaction()
  static async testTx(arg: string, rv?: string, id?: string): Promise<string> {
    expect(arg).toBe('a');
    if (id) {
      expect(DBOS.workflowID).toBe(id);
    }
    ++StaticStepTx.txCnt;
    return Promise.resolve(rv ?? '');
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

  // Test that functions run
  test('run-step-tx', async () => {
    expect(await StaticStepTx.testTx('a', '1')).toBe('1');
    expect(await StaticStepTx.testStep('a', '1')).toBe('1');
    expect(await inst.testTx('a', '1')).toBe('1');
    expect(await inst.testStep('a', '1')).toBe('1');

    expect(StaticStepTx.stepCnt).toBe(1);
    expect(StaticStepTx.txCnt).toBe(1);
    expect(InstanceStepTx.stepCnt).toBe(1);
    expect(InstanceStepTx.txCnt).toBe(1);
  }, 10000);

  // Test that functions run as workflows
  test('start-step-tx', async () => {
    expect(await (await DBOS.startWorkflow(StaticStepTx).testTx('a', '1')).getResult()).toBe('1');
    expect(await (await DBOS.startWorkflow(StaticStepTx).testStep('a', '1')).getResult()).toBe('1');
    expect(await (await DBOS.startWorkflow(inst).testTx('a', '1')).getResult()).toBe('1');
    expect(await (await DBOS.startWorkflow(inst).testStep('a', '1')).getResult()).toBe('1');

    expect(StaticStepTx.stepCnt).toBe(1);
    expect(StaticStepTx.txCnt).toBe(1);
    expect(InstanceStepTx.stepCnt).toBe(1);
    expect(InstanceStepTx.txCnt).toBe(1);
  }, 10000);

  // Test that functions run as workflows w/ assigned IDs
  test('start-step-tx-wfid', async () => {
    expect(
      await (await DBOS.startWorkflow(StaticStepTx, { workflowID: 'wfst1' }).testTx('a', '1', 'wfst1')).getResult(),
    ).toBe('1');
    expect(
      await (await DBOS.startWorkflow(StaticStepTx, { workflowID: 'wfst2' }).testStep('a', '1', 'wfst2')).getResult(),
    ).toBe('1');
    expect(await (await DBOS.startWorkflow(inst, { workflowID: 'wfst3' }).testTx('a', '1', 'wfst3')).getResult()).toBe(
      '1',
    );
    expect(
      await (await DBOS.startWorkflow(inst, { workflowID: 'wfst4' }).testStep('a', '1', 'wfst4')).getResult(),
    ).toBe('1');

    expect(StaticStepTx.stepCnt).toBe(1);
    expect(StaticStepTx.txCnt).toBe(1);
    expect(InstanceStepTx.stepCnt).toBe(1);
    expect(InstanceStepTx.txCnt).toBe(1);
  }, 10000);
});
