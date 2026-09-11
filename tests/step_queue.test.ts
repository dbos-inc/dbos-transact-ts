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
import { randomUUID } from 'node:crypto';

const queue = { name: 'testQ' };

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

class WorkflowsCallingSteps {
  @DBOS.workflow()
  static async runFuncs() {
    expect(await StaticStep.testStep('a', '1')).toBe('1');
    expect(await inst.testStep('a', '1')).toBe('1');
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
    for (const ref of [queue, TestQueueRecoveryInst.queue]) {
      await DBOS.registerQueue(ref.name, { onConflict: 'always_update' });
    }
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  // Test that steps called outside a workflow run, without checkpointing anything
  test('run-step-tx', async () => {
    const wfsBefore = (await DBOS.listWorkflows({})).length;

    expect(await StaticStep.testStep('a', '1')).toBe('1');
    expect(await inst.testStep('a', '1')).toBe('1');

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
    expect((await DBOS.listWorkflows({})).length - wfsBefore).toBe(0);
  });

  // An assigned workflow ID belongs to the next workflow; a step in the closure leaves it alone
  test('step-does-not-consume-assigned-id', async () => {
    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      expect(await StaticStep.testStep('a', '1')).toBe('1');
      expect(await inst.testStep('a', '1')).toBe('1');
      const handle = await DBOS.startWorkflow(WorkflowsCallingSteps).runFuncs();
      expect(handle.workflowID).toBe(wfid);
      await handle.getResult();
    });

    // Two plain calls here, plus the pair the workflow itself makes.
    expect(StaticStep.stepCnt).toBe(2);
    expect(InstanceStep.stepCnt).toBe(2);
  });

  // Steps are not workflows: they can be neither started nor enqueued
  test('start-step-rejected', async () => {
    await expect(DBOS.startWorkflow(StaticStep).testStep('a', '1')).rejects.toThrow(
      /only workflows can be started or enqueued/,
    );
    await expect(DBOS.startWorkflow(inst).testStep('a', '1')).rejects.toThrow(
      /only workflows can be started or enqueued/,
    );

    expect(StaticStep.stepCnt).toBe(0);
    expect(InstanceStep.stepCnt).toBe(0);
  });

  test('enqueue-step-rejected', async () => {
    await expect(DBOS.startWorkflow(StaticStep, { queueName: queue.name }).testStep('a', '1')).rejects.toThrow(
      /only workflows can be started or enqueued/,
    );
    await expect(DBOS.startWorkflow(inst, { queueName: queue.name }).testStep('a', '1')).rejects.toThrow(
      /only workflows can be started or enqueued/,
    );

    expect(StaticStep.stepCnt).toBe(0);
    expect(InstanceStep.stepCnt).toBe(0);
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });

  // Test that functions run (from wf)
  test('run-step-tx-wf', async () => {
    await WorkflowsCallingSteps.runFuncs();

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
  });

  // Test that functions run (from wf)
  test('run-step-tx-wf-onq', async () => {
    const wfh1 = await DBOS.startWorkflow(WorkflowsCallingSteps, { queueName: queue.name }).runFuncs();
    await wfh1.getResult();

    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });

  test('test-queue-recovery', async () => {
    const wfid = randomUUID();

    // Start the workflow. Wait for all five tasks to start. Verify that they started.
    const originalHandle = await DBOS.startWorkflow(tqrInst, { workflowID: wfid }).testWorkflow();
    for (const e of tqrInst.taskEvents) {
      await e.wait();
      e.clear();
    }
    expect(tqrInst.taskCount).toEqual(TestQueueRecoveryInst.queuedTasks);
    await originalHandle.getResult();

    // Recover the workflow, then resume it. There should be one handle for the workflow and another for each task.
    await setWfAndChildrenToPending(originalHandle.workflowID);
    const recoveryHandles = await recoverPendingWorkflows();
    expect(recoveryHandles.length).toBe(TestQueueRecoveryInst.queuedTasks + 1);

    // Verify both the recovered and original workflows complete correctly
    for (const h of recoveryHandles) {
      if (h.workflowID === wfid) {
        await expect(h.getResult()).resolves.toEqual(
          Array.from({ length: TestQueueRecoveryInst.queuedTasks }, (_, i) => i),
        );
      }
    }
    await expect(originalHandle.getResult()).resolves.toEqual(
      Array.from({ length: TestQueueRecoveryInst.queuedTasks }, (_, i) => i),
    );

    // Each task should start once, recovery doesn't rerun because they are checkpointed
    expect(tqrInst.taskCount).toEqual(1 * TestQueueRecoveryInst.queuedTasks);

    // Verify all queue entries eventually get cleaned up
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });
});

class TestQueueRecoveryInst extends ConfiguredInstance {
  constructor() {
    super('single');
  }
  initialize(): Promise<void> {
    return Promise.resolve();
  }
  static queuedTasks = 3;
  taskEvents = Array.from({ length: TestQueueRecoveryInst.queuedTasks }, () => new Event());
  taskCount = 0;
  static queue = { name: 'testQueueRecovery' };

  @DBOS.workflow()
  async testWorkflow() {
    const handles: WorkflowHandle<number>[] = [];
    for (let i = 0; i < TestQueueRecoveryInst.queuedTasks; i++) {
      const h = await DBOS.startWorkflow(this, { queueName: TestQueueRecoveryInst.queue.name }).blockingTask(i);
      handles.push(h);
    }
    const results: number[] = [];
    for (const h of handles) results.push(await h.getResult());
    return results;
  }

  @DBOS.workflow()
  async blockingTask(i: number) {
    return await this.countTask(i);
  }

  // The count lives in a step so recovery replays the checkpoint instead of running it again
  @DBOS.step()
  async countTask(i: number) {
    this.taskEvents[i].set();
    this.taskCount++;
    return Promise.resolve(i);
  }
}

const tqrInst = new TestQueueRecoveryInst();
