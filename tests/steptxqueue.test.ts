import { DBOS, ConfiguredInstance, InitContext } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { WorkflowQueue } from '../src';

const queue = new WorkflowQueue('testQ');
const serialqueue = new WorkflowQueue('serialQ', 1);

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

class WorkflowsEnqueue {
  @DBOS.workflow()
  static async runFuncs() {
    expect(await StaticStepTx.testTx('a', '1')).toBe('1');
    expect(await StaticStepTx.testStep('a', '1')).toBe('1');
    expect(await inst.testTx('a', '1')).toBe('1');
    expect(await inst.testStep('a', '1')).toBe('1');
  }

  @DBOS.workflow()
  static async runAsWFs() {
    expect(await (await DBOS.startWorkflow(StaticStepTx).testTx('a', '1')).getResult()).toBe('1');
    expect(await (await DBOS.startWorkflow(StaticStepTx).testStep('a', '1')).getResult()).toBe('1');
    expect(await (await DBOS.startWorkflow(inst).testTx('a', '1')).getResult()).toBe('1');
    expect(await (await DBOS.startWorkflow(inst).testStep('a', '1')).getResult()).toBe('1');
  }

  @DBOS.workflow()
  static async runAsWFIDs() {
    expect(
      await (
        await DBOS.startWorkflow(StaticStepTx, { workflowID: 'wwfstq1', queueName: serialqueue.name }).testTx(
          'a',
          '1',
          'wwfstq1',
        )
      ).getResult(),
    ).toBe('1');
    expect(
      await (
        await DBOS.startWorkflow(StaticStepTx, { workflowID: 'wwfstq2', queueName: serialqueue.name }).testStep(
          'a',
          '1',
          'wwfstq2',
        )
      ).getResult(),
    ).toBe('1');
    expect(
      await (
        await DBOS.startWorkflow(inst, { workflowID: 'wwfstq3', queueName: serialqueue.name }).testTx(
          'a',
          '1',
          'wwfstq3',
        )
      ).getResult(),
    ).toBe('1');
    expect(
      await (
        await DBOS.startWorkflow(inst, { workflowID: 'wwfstq4', queueName: serialqueue.name }).testStep(
          'a',
          '1',
          'wwfstq4',
        )
      ).getResult(),
    ).toBe('1');
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

  // Test that functions run as workflows w/ assigned IDs and q
  test('start-step-tx-wfid', async () => {
    expect(
      await (
        await DBOS.startWorkflow(StaticStepTx, { workflowID: 'wfstq1', queueName: queue.name }).testTx(
          'a',
          '1',
          'wfstq1',
        )
      ).getResult(),
    ).toBe('1');
    expect(
      await (
        await DBOS.startWorkflow(StaticStepTx, { workflowID: 'wfstq2', queueName: queue.name }).testStep(
          'a',
          '1',
          'wfstq2',
        )
      ).getResult(),
    ).toBe('1');
    expect(
      await (
        await DBOS.startWorkflow(inst, { workflowID: 'wfstq3', queueName: queue.name }).testTx('a', '1', 'wfstq3')
      ).getResult(),
    ).toBe('1');
    expect(
      await (
        await DBOS.startWorkflow(inst, { workflowID: 'wfstq4', queueName: queue.name }).testStep('a', '1', 'wfstq4')
      ).getResult(),
    ).toBe('1');

    const wfh1 = DBOS.retrieveWorkflow('wfstq1');
    expect((await wfh1.getStatus())?.queueName).toBe(queue.name);
    const wfh2 = DBOS.retrieveWorkflow('wfstq2');
    expect((await wfh2.getStatus())?.queueName).toBe(queue.name);
    const wfh3 = DBOS.retrieveWorkflow('wfstq3');
    expect((await wfh3.getStatus())?.queueName).toBe(queue.name);
    const wfh4 = DBOS.retrieveWorkflow('wfstq4');
    expect((await wfh4.getStatus())?.queueName).toBe(queue.name);

    expect(StaticStepTx.stepCnt).toBe(1);
    expect(StaticStepTx.txCnt).toBe(1);
    expect(InstanceStepTx.stepCnt).toBe(1);
    expect(InstanceStepTx.txCnt).toBe(1);
  }, 10000);

  // Test that functions run (from wf)
  test('run-step-tx-wf', async () => {
    await WorkflowsEnqueue.runFuncs();

    expect(StaticStepTx.stepCnt).toBe(1);
    expect(StaticStepTx.txCnt).toBe(1);
    expect(InstanceStepTx.stepCnt).toBe(1);
    expect(InstanceStepTx.txCnt).toBe(1);
  }, 10000);

  // Test that functions run as child WFs (from wf)
  test('run-step-tx-cwf', async () => {
    await WorkflowsEnqueue.runAsWFs();

    expect(StaticStepTx.stepCnt).toBe(1);
    expect(StaticStepTx.txCnt).toBe(1);
    expect(InstanceStepTx.stepCnt).toBe(1);
    expect(InstanceStepTx.txCnt).toBe(1);
  }, 10000);

  // Test that functions run as child WFs (from wf)
  test('run-step-tx-wfq', async () => {
    await WorkflowsEnqueue.runAsWFIDs();

    const wfh1 = DBOS.retrieveWorkflow('wwfstq1');
    expect((await wfh1.getStatus())?.queueName).toBe(serialqueue.name);
    const wfh2 = DBOS.retrieveWorkflow('wwfstq2');
    expect((await wfh2.getStatus())?.queueName).toBe(serialqueue.name);
    const wfh3 = DBOS.retrieveWorkflow('wwfstq3');
    expect((await wfh3.getStatus())?.queueName).toBe(serialqueue.name);
    const wfh4 = DBOS.retrieveWorkflow('wwfstq4');
    expect((await wfh4.getStatus())?.queueName).toBe(serialqueue.name);

    expect(StaticStepTx.stepCnt).toBe(1);
    expect(StaticStepTx.txCnt).toBe(1);
    expect(InstanceStepTx.stepCnt).toBe(1);
    expect(InstanceStepTx.txCnt).toBe(1);
  }, 30000);
});
