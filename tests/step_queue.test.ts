import { DBOS, ConfiguredInstance, WorkflowHandle } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import {
  generateDBOSTestConfig,
  queueEntriesAreCleanedUp,
  setUpDBOSTestSysDb,
  Event,
  recoverPendingWorkflows,
  setWfAndChildrenToPending,
} from './helpers';
import { WorkflowQueue } from '../src';
import { randomUUID } from 'node:crypto';

const queue = new WorkflowQueue('testQ');
const serialqueue = new WorkflowQueue('serialQ', { concurrency: 1 });

class InstanceStep extends ConfiguredInstance {
  constructor() {
    super('Instance');
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  @DBOS.step()
  async testStep(arg: string, rv?: string, id?: string): Promise<string> {
    expect(arg).toBe('a');
    if (id) {
      expect(DBOS.workflowID).toBe(id);
    }
    ++InstanceStep.stepCnt;
    return Promise.resolve(rv ?? '');
  }

  static stepCnt = 0;
  static reset() {
    InstanceStep.stepCnt = 0;
  }
}

const inst = new InstanceStep();

class StaticStep extends ConfiguredInstance {
  constructor() {
    super('Instance');
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  @DBOS.step()
  static async testStep(arg: string, rv?: string, id?: string): Promise<string> {
    expect(arg).toBe('a');
    if (id) {
      expect(DBOS.workflowID).toBe(id);
    }
    ++StaticStep.stepCnt;
    return Promise.resolve(rv ?? '');
  }

  static stepCnt = 0;
  static reset() {
    StaticStep.stepCnt = 0;
  }
}

class WorkflowsEnqueue {
  @DBOS.workflow()
  static async runFuncs() {
    expect(await StaticStep.testStep('a', '1')).toBe('1');
    expect(await inst.testStep('a', '1')).toBe('1');
  }

  @DBOS.workflow()
  static async runAsWFs() {
    expect(await (await DBOS.startWorkflow(StaticStep).testStep('a', '1')).getResult()).toBe('1');
    expect(await (await DBOS.startWorkflow(inst).testStep('a', '1')).getResult()).toBe('1');
  }

  @DBOS.workflow()
  static async runAsWFIDs(base: string = 'wwfstq') {
    expect(
      await (
        await DBOS.startWorkflow(StaticStep, { workflowID: `${base}2`, queueName: serialqueue.name }).testStep(
          'a',
          '1',
          `${base}2`,
        )
      ).getResult(),
    ).toBe('1');
    expect(
      await (
        await DBOS.startWorkflow(inst, { workflowID: `${base}4`, queueName: serialqueue.name }).testStep(
          'a',
          '1',
          `${base}4`,
        )
      ).getResult(),
    ).toBe('1');
  }
}

describe('queued-wf-tests-simple', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    StaticStep.reset();
    InstanceStep.reset();
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  // Test that functions run
  test('run-step-tx', async () => {
    expect(await StaticStep.testStep('a', '1')).toBe('1');
    expect(await inst.testStep('a', '1')).toBe('1');

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
  }, 10000);

  // Test that functions run as workflows
  test('start-step-tx', async () => {
    expect(await (await DBOS.startWorkflow(StaticStep).testStep('a', '1')).getResult()).toBe('1');
    expect(await (await DBOS.startWorkflow(inst).testStep('a', '1')).getResult()).toBe('1');

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
  }, 10000);

  // Test that functions run as workflows w/ assigned IDs
  test('start-step-tx-wfid', async () => {
    expect(
      await (await DBOS.startWorkflow(StaticStep, { workflowID: 'wfst2' }).testStep('a', '1', 'wfst2')).getResult(),
    ).toBe('1');
    expect(
      await (await DBOS.startWorkflow(inst, { workflowID: 'wfst4' }).testStep('a', '1', 'wfst4')).getResult(),
    ).toBe('1');

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
  }, 10000);

  // Test that functions run as workflows w/ assigned IDs and q
  test('start-step-tx-wfid', async () => {
    expect(
      await (
        await DBOS.startWorkflow(StaticStep, { workflowID: 'wfstq2', queueName: queue.name }).testStep(
          'a',
          '1',
          'wfstq2',
        )
      ).getResult(),
    ).toBe('1');
    expect(
      await (
        await DBOS.startWorkflow(inst, { workflowID: 'wfstq4', queueName: queue.name }).testStep('a', '1', 'wfstq4')
      ).getResult(),
    ).toBe('1');

    const wfh2 = DBOS.retrieveWorkflow('wfstq2');
    expect((await wfh2.getStatus())?.queueName).toBe(queue.name);
    const wfh4 = DBOS.retrieveWorkflow('wfstq4');
    expect((await wfh4.getStatus())?.queueName).toBe(queue.name);

    expect(await queueEntriesAreCleanedUp()).toBe(true);

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
  }, 10000);

  // Test that functions run (from wf)
  test('run-step-tx-wf', async () => {
    await WorkflowsEnqueue.runFuncs();

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
  }, 10000);

  // Test that functions run as child WFs (from wf)
  test('run-step-tx-cwf', async () => {
    await WorkflowsEnqueue.runAsWFs();

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
  }, 10000);

  // Test that functions run as child WFs (from wf)
  test('run-step-tx-wfq', async () => {
    await WorkflowsEnqueue.runAsWFIDs();

    const wfh2 = DBOS.retrieveWorkflow('wwfstq2');
    expect((await wfh2.getStatus())?.queueName).toBe(serialqueue.name);
    const wfh4 = DBOS.retrieveWorkflow('wwfstq4');
    expect((await wfh4.getStatus())?.queueName).toBe(serialqueue.name);

    expect(await queueEntriesAreCleanedUp()).toBe(true);

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
  }, 30000);

  // Test that functions run (from wf)
  test('run-step-tx-wf-onq', async () => {
    const wfh1 = await DBOS.startWorkflow(WorkflowsEnqueue, { queueName: queue.name }).runFuncs();
    const wfh2 = await DBOS.startWorkflow(WorkflowsEnqueue, { queueName: queue.name }).runAsWFs();
    const wfh3 = await DBOS.startWorkflow(WorkflowsEnqueue, { queueName: queue.name }).runAsWFIDs('qwfsfromwfs');

    await wfh1.getResult();
    await wfh2.getResult();
    await wfh3.getResult();

    expect(StaticStep.stepCnt).toBe(3);
    expect(InstanceStep.stepCnt).toBe(3);
  }, 10000);

  test('test-queue-recovery', async () => {
    const wfid = randomUUID();

    console.log('GH1');
    // Start the workflow. Wait for all five tasks to start. Verify that they started.
    const originalHandle = await DBOS.startWorkflow(tqrInst, { workflowID: wfid }).testWorkflow();
    for (const e of tqrInst.taskEvents) {
      await e.wait();
      e.clear();
    }
    expect(tqrInst.taskCount).toEqual(TestQueueRecoveryInst.queuedSteps);
    await originalHandle.getResult();
    console.log('GH2');

    // Recover the workflow, then resume it. There should be one handle for the workflow and another for each task.
    await setWfAndChildrenToPending(originalHandle.workflowID);
    const recoveryHandles = await recoverPendingWorkflows();
    expect(recoveryHandles.length).toBe(TestQueueRecoveryInst.queuedSteps + 1);
    console.log('GH3');

    // Verify both the recovered and original workflows complete correctly
    for (const h of recoveryHandles) {
      if (h.workflowID === wfid) {
        await expect(h.getResult()).resolves.toEqual(
          Array.from({ length: TestQueueRecoveryInst.queuedSteps }, (_, i) => i),
        );
      }
    }
    await expect(originalHandle.getResult()).resolves.toEqual(
      Array.from({ length: TestQueueRecoveryInst.queuedSteps }, (_, i) => i),
    );
    console.log('GH4');

    // Each task should start once, recovery doesn't rerun because they are checkpointed
    expect(tqrInst.taskCount).toEqual(1 * TestQueueRecoveryInst.queuedSteps);

    // Verify all queue entries eventually get cleaned up
    expect(await queueEntriesAreCleanedUp()).toBe(true);
    console.log('GH5');
  }, 10000);
});

class TestQueueRecoveryInst extends ConfiguredInstance {
  constructor() {
    super('single');
  }
  initialize(): Promise<void> {
    return Promise.resolve();
  }
  static queuedSteps = 3;
  taskEvents = Array.from({ length: TestQueueRecoveryInst.queuedSteps }, () => new Event());
  taskCount = 0;
  static queue = new WorkflowQueue('testQueueRecovery');

  @DBOS.workflow()
  async testWorkflow() {
    const handles: WorkflowHandle<number>[] = [];
    for (let i = 0; i < TestQueueRecoveryInst.queuedSteps; i++) {
      const h = await DBOS.startWorkflow(this, { queueName: TestQueueRecoveryInst.queue.name }).blockingTask(i);
      handles.push(h);
    }
    const results: number[] = [];
    for (const h of handles) results.push(await h.getResult());
    return results;
  }

  @DBOS.step()
  async blockingTask(i: number) {
    this.taskEvents[i].set();
    this.taskCount++;
    return Promise.resolve(i);
  }
}

const tqrInst = new TestQueueRecoveryInst();
