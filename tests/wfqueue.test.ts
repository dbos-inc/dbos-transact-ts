import { StatusString, WorkflowHandle, DBOS, ConfiguredInstance, DBOSClient } from '../src';
import { DBOSConfig, DBOSExecutor, DBOS_QUEUE_MAX_PRIORITY, DBOS_QUEUE_MIN_PRIORITY } from '../src/dbos-executor';
import { QueueParameters, wfQueueRunner } from '../src/wfqueue';
import {
  generateDBOSTestConfig,
  setUpDBOSTestSysDb,
  Event,
  queueEntriesAreCleanedUp,
  recoverPendingWorkflows,
  reexecuteWorkflowById,
  setWfAndChildrenToPending,
} from './helpers';
import { WorkflowQueue } from '../src';
import { randomUUID } from 'node:crypto';
import { globalParams, sleepms } from '../src/utils';

import { WF } from './wfqtestprocess';

import { execFile, spawn } from 'child_process';
import { promisify } from 'util';
import { Client } from 'pg';
import {
  DBOSInvalidQueuePriorityError,
  DBOSConflictingWorkflowError,
  DBOSQueueDuplicatedError,
  DBOSAwaitedWorkflowCancelledError,
  QueueDedupIDDuplicated,
  getDBOSErrorCode,
} from '../src/error';

const execFileAsync = promisify(execFile);

import {
  clearDebugTriggers,
  DEBUG_TRIGGER_WORKFLOW_QUEUE_START,
  DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES,
  DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT,
  // DEBUG_TRIGGER_WORKFLOW_ENQUEUE,
  setDebugTrigger,
} from '../src/debugpoint';
import assert from 'node:assert';

const testPolling = { minPollingIntervalMs: 100 };

// Queue descriptors. The actual database-backed queues are registered in
// `beforeEach` via `DBOS.registerQueue` once DBOS has launched. These objects
// keep `queue.name` available for use in `DBOS.startWorkflow` calls.
type QueueRef = { name: string; config: QueueParameters };
const queue: QueueRef = { name: 'testQ', config: { ...testPolling } };
const serialqueue: QueueRef = { name: 'serialQ', config: { concurrency: 1, ...testPolling } };
const serialqueueLimited: QueueRef = {
  name: 'serialQL',
  config: { concurrency: 1, rateLimit: { limitPerPeriod: 10, periodSec: 1 }, ...testPolling },
};
const childqueue: QueueRef = { name: 'childQ', config: { concurrency: 3, ...testPolling } };
const workerConcurrencyQueue: QueueRef = {
  name: 'workerQ',
  config: { workerConcurrency: 1, ...testPolling },
};

const qlimit = 5;
const qperiod = 2;
const rlqueue: QueueRef = {
  name: 'limited_queue',
  config: { rateLimit: { limitPerPeriod: qlimit, periodSec: qperiod } },
};

const sharedQueueRefs: QueueRef[] = [
  queue,
  serialqueue,
  serialqueueLimited,
  childqueue,
  workerConcurrencyQueue,
  rlqueue,
];

async function registerSharedQueues() {
  for (const ref of sharedQueueRefs) {
    await DBOS.registerQueue(ref.name, { onConflict: 'always_update', ...ref.config });
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
    TestWFs.reset();
    TestWFs2.reset();
    await DBOS.launch();
    await registerSharedQueues();
    // Class-level queues used inside this describe block.
    for (const ref of [
      TestQueueRecovery.queue,
      TestQueueRecovery.recoveryQueue,
      TestCancelQueues.queue,
      TestResumeQueues.queue,
      TestResumeQueuesPartitioned.queue,
      TestConcurrencyAcrossVersions.queue,
      waitFirstQueue,
    ]) {
      await DBOS.registerQueue(ref.name, { onConflict: 'always_update', ...ref.config });
    }
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-queue', async () => {
    const wfid = randomUUID();
    TestWFs.wfid = wfid;

    const wfh = await DBOS.startWorkflow(TestWFs, { workflowID: wfid, queueName: queue.name }).testWorkflow(
      'abc',
      '123',
    );
    expect(await wfh.getResult()).toBe('abcd123');
    expect((await wfh.getStatus())?.queueName).toBe('testQ');

    await DBOS.withNextWorkflowID(wfid, async () => {
      expect(await TestWFs.testWorkflow('abc', '123')).toBe('abcd123');
    });
    expect(TestWFs.wfCounter).toBe(1);
    expect(TestWFs.stepCounter).toBe(1);

    expect((await wfh.getStatus())?.queueName).toBe('testQ');
  });

  test('one-at-a-time', async () => {
    await runOneAtATime(serialqueue);
  });

  test('child-wfs-queue', async () => {
    expect(await TestChildWFs.testWorkflow('a', 'b')).toBe('adbdadbd');
  });

  test('test_one_at_a_time_with_limiter', async () => {
    await runOneAtATime(serialqueueLimited);
  });

  test('test_one_at_a_time_with_worker_concurrency', async () => {
    await runOneAtATime(workerConcurrencyQueue);
  });

  test('test-queue_rate_limit', async () => {
    const handles: WorkflowHandle<number>[] = [];
    const times: number[] = [];

    // Launch a number of tasks equal to three times the limit.
    // This should lead to three "waves" of the limit tasks being
    //   executed simultaneously, followed by a wait of the period,
    //   followed by the next wave.
    const numWaves = 3;

    for (let i = 0; i < qlimit * numWaves; ++i) {
      const h = await DBOS.startWorkflow(TestWFs, { queueName: rlqueue.name }).testWorkflowTime('abc', '123');
      handles.push(h);
    }
    for (const h of handles) {
      times.push(await h.getResult());
    }

    // Verify all queue entries eventually get cleaned up.
    expect(await queueEntriesAreCleanedUp()).toBe(true);

    // Verify that each "wave" of tasks started at the ~same time.
    for (let wave = 0; wave < numWaves; ++wave) {
      for (let i = wave * qlimit; i < (wave + 1) * qlimit - 1; ++i) {
        expect(times[i + 1] - times[i]).toBeLessThan(1100);
      }
    }

    // Verify that the gap between "waves" is ~equal to the period
    for (let wave = 1; wave < numWaves; ++wave) {
      expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeGreaterThan(qperiod * 1000 - 300);
      expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeLessThan(qperiod * 1000 * 2);
    }

    for (const h of handles) {
      expect((await h.getStatus())!.status).toBe(StatusString.SUCCESS);
    }

    // Workflows dequeued from a rate-limited queue must have rate_limited = TRUE,
    // so the partial idx_workflow_status_rate_limited index covers the count query.
    const sysDbUrl = generateDBOSTestConfig().systemDatabaseUrl!;
    const c = new Client({ connectionString: sysDbUrl });
    await c.connect();
    try {
      const rl = await c.query<{ count: string }>(
        `SELECT COUNT(*) FROM dbos.workflow_status WHERE queue_name = $1 AND rate_limited = TRUE`,
        [rlqueue.name],
      );
      expect(Number(rl.rows[0].count)).toBe(qlimit * numWaves);
    } finally {
      await c.end();
    }
  });

  test('test_multiple_queues', async () => {
    let wfRes: () => void = () => {};
    TestWFs2.wfPromise = new Promise<void>((resolve, _rj) => {
      wfRes = resolve;
    });
    const mainPromise = new Promise<void>((resolve, _rj) => {
      TestWFs2.mainResolve = resolve;
    });

    const wfh1 = await DBOS.startWorkflow(TestWFs2, { queueName: serialqueue.name }).workflowOne();
    expect((await wfh1.getStatus())?.queueName).toBe(serialqueue.name);
    const wfh2 = await DBOS.startWorkflow(TestWFs2, { queueName: serialqueue.name }).workflowTwo();
    expect((await wfh2.getStatus())?.queueName).toBe(serialqueue.name);
    // At this point Wf2 is stuck.

    const handles: WorkflowHandle<number>[] = [];
    const times: number[] = [];

    // Launch a number of tasks equal to three times the limit.
    // This should lead to three "waves" of the limit tasks being
    //   executed simultaneously, followed by a wait of the period,
    //   followed by the next wave.
    const numWaves = 3;

    for (let i = 0; i < qlimit * numWaves; ++i) {
      const h = await DBOS.startWorkflow(TestWFs, { queueName: rlqueue.name }).testWorkflowTime('abc', '123');
      handles.push(h);
    }
    for (const h of handles) {
      times.push(await h.getResult());
    }

    // Verify that each "wave" of tasks started at the ~same time.
    for (let wave = 0; wave < numWaves; ++wave) {
      for (let i = wave * qlimit; i < (wave + 1) * qlimit - 1; ++i) {
        expect(times[i + 1] - times[i]).toBeLessThan(1100);
      }
    }

    // Verify that the gap between "waves" is ~equal to the period
    for (let wave = 1; wave < numWaves; ++wave) {
      expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeGreaterThan(qperiod * 1000 - 300);
      expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeLessThan(qperiod * 1000 * 2);
    }

    for (const h of handles) {
      expect((await h.getStatus())!.status).toBe(StatusString.SUCCESS);
    }

    // Verify that during all this time, the second task
    //   was not launched on the concurrency-limited queue.
    // Then, finish the first task and verify the second
    //   task runs on schedule.
    await mainPromise;
    await sleepms(2000);
    expect(TestWFs2.flag).toBeFalsy();
    wfRes?.();
    await wfh1.getResult();
    await wfh2.getResult();
    expect(TestWFs2.flag).toBeTruthy();
    expect(TestWFs2.wfCounter).toBe(1);

    // Verify all queue entries eventually get cleaned up.
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });

  test('test_one_at_a_time_with_crash', async () => {
    let wfqRes: () => void = () => {};
    const wfqPromise = new Promise<void>((resolve, _rj) => {
      wfqRes = resolve;
    });

    setDebugTrigger(DEBUG_TRIGGER_WORKFLOW_QUEUE_START, {
      callback: () => {
        wfqRes();
        throw new Error('Interrupt scheduler here');
      },
    });

    const wfh1 = await DBOS.startWorkflow(TestWFs, { queueName: serialqueue.name }).testWorkflowSimple('a', 'b');
    await wfqPromise;

    await DBOS.shutdown();
    clearDebugTriggers();
    await DBOS.launch();

    const wfh2 = await DBOS.startWorkflow(TestWFs, { queueName: serialqueue.name }).testWorkflowSimple('c', 'd');

    const wfh1b = DBOS.retrieveWorkflow(wfh1.workflowID);
    const wfh2b = DBOS.retrieveWorkflow(wfh2.workflowID);
    expect(await wfh1b.getResult()).toBe('ab');
    expect(await wfh2b.getResult()).toBe('cd');
  });

  test('queue workflow in recovered workflow', async () => {
    expect(WF.x).toBe(5);
    console.log('shutdown');
    const appVersion = globalParams.appVersion;
    await DBOS.shutdown(); // DO not want to take queued jobs from here

    console.log('run side process');
    // We crash a workflow on purpose; this has queued some things up and awaited them...
    const { stdout, stderr } = await execFileAsync('npx', ['ts-node', './tests/wfqtestprocess.ts'], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        DIE_ON_PURPOSE: 'true',
        DBOS__APPVERSION: appVersion,
      },
    });

    expect(stderr).toBeDefined();
    expect(stdout).toBeDefined();
    console.log(stdout);

    console.log('start again');
    await DBOS.launch();
    const wfh = DBOS.retrieveWorkflow('testqueuedwfcrash');
    expect((await wfh.getStatus())?.status).toBe('PENDING');

    // It should proceed.  And should not take too long, either...
    //  We could also recover the workflow
    console.log('Waiting for recovered WF to complete...');
    expect(await wfh.getResult()).toBe(5);

    expect((await wfh.getStatus())?.status).toBe('SUCCESS');
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  }, 60000);

  class TestDuplicateID {
    @DBOS.workflow()
    static async testWorkflow(var1: string) {
      await DBOS.sleepms(10);
      return var1;
    }

    @DBOS.workflow()
    static async testDupWorkflow() {
      await DBOS.sleepms(10);
      return;
    }
  }

  class TestDuplicateIDdup {
    @DBOS.workflow()
    static async testWorkflow(var1: string) {
      await DBOS.sleepms(10);
      return var1;
    }
  }

  class TestDuplicateIDins extends ConfiguredInstance {
    constructor(name: string) {
      super(name);
    }

    async initialize() {
      return Promise.resolve();
    }

    @DBOS.workflow()
    async testWorkflow(var1: string) {
      await DBOS.sleepms(10);
      return var1;
    }
  }

  test('duplicate-workflow-id', async () => {
    const wfid = randomUUID();
    const handle1 = await DBOS.startWorkflow(TestDuplicateID, { workflowID: wfid }).testWorkflow('abc');
    // Call with a different function name within the same class is not allowed.
    await expect(DBOS.startWorkflow(TestDuplicateID, { workflowID: wfid }).testDupWorkflow()).rejects.toThrow(
      DBOSConflictingWorkflowError,
    );
    // Call the same function name in a different class is not allowed.
    await expect(DBOS.startWorkflow(TestDuplicateIDdup, { workflowID: wfid }).testWorkflow('abc')).rejects.toThrow(
      DBOSConflictingWorkflowError,
    );
    await expect(handle1.getResult()).resolves.toBe('abc');

    // Calling itself again should be fine
    const handle2 = await DBOS.startWorkflow(TestDuplicateID, { workflowID: wfid }).testWorkflow('abc');
    await expect(handle2.getResult()).resolves.toBe('abc');

    // Call the same function in a different configured class is not allowed.
    const myObj = new TestDuplicateIDins('myname');
    await expect(DBOS.startWorkflow(myObj, { workflowID: wfid }).testWorkflow('abc')).rejects.toThrow(
      DBOSConflictingWorkflowError,
    );

    // Call the same function in a different queue would generate a warning, but is allowed.
    const handleQ = await DBOS.startWorkflow(TestDuplicateID, { workflowID: wfid, queueName: queue.name }).testWorkflow(
      'abc',
    );
    await expect(handleQ.getResult()).resolves.toBe('abc');

    // Call with a different input would generate a warning, but still use the recorded input.
    const handle3 = await DBOS.startWorkflow(TestDuplicateID, { workflowID: wfid }).testWorkflow('def');
    await expect(handle3.getResult()).resolves.toBe('abc');
  });

  class TestQueueRecovery {
    static queuedSteps = 5;
    static taskEvents = Array.from({ length: TestQueueRecovery.queuedSteps }, () => new Event());
    static taskCount = 0;
    static queue: QueueRef = { name: 'testQueueRecovery', config: { ...testPolling } };
    static recoveryQueue: QueueRef = { name: 'recoveryQ', config: { concurrency: 2, ...testPolling } };

    @DBOS.workflow()
    static async testWorkflow() {
      const handles: WorkflowHandle<number>[] = [];
      for (let i = 0; i < TestQueueRecovery.queuedSteps; i++) {
        const h = await DBOS.startWorkflow(TestQueueRecovery, { queueName: TestQueueRecovery.queue.name }).blockingTask(
          i,
        );
        handles.push(h);
      }

      // NOTE:
      // The code below used to say:
      //  return Promise.all(handles.map((h) => h.getResult()));
      // This is not broken _per se_, but it does interact quite badly with the test
      //  below that intentionally runs the workflow concurrently.  Promise.all
      //  will run the getResult calls concurrently.  In turn, they will all be in a
      //  race to record their results in system DB.  The system DB reacts by killing
      //  (throwing a DBOSWorkflowConflictError) from the workflows that conflict, and
      //  there's a high probability that this is both of them, as there are 5 separate
      //  races here (to record each of the 5 results).  Boom! (with 15/16 probability).
      const results: number[] = [];
      for (const h of handles) results.push(await h.getResult());
      return results;
    }

    @DBOS.workflow()
    static async blockingTask(i: number) {
      TestQueueRecovery.taskEvents[i].set();
      TestQueueRecovery.taskCount++;
      return Promise.resolve(i);
    }

    static cnt = 0;
    static blockedWorkflows = 2;
    static startEvents = Array.from({ length: TestQueueRecovery.blockedWorkflows }, () => new Event());
    static stopEvent = new Event();
    @DBOS.workflow()
    static async blockedWorkflow(i: number) {
      TestQueueRecovery.startEvents[i].set();
      TestQueueRecovery.cnt++;
      await TestQueueRecovery.stopEvent.wait();
    }
  }

  test('test-queue-recovery', async () => {
    const wfid = randomUUID();

    // Start the workflow. Wait for all five tasks to start. Verify that they started.
    const originalHandle = await DBOS.startWorkflow(TestQueueRecovery, { workflowID: wfid }).testWorkflow();
    for (const e of TestQueueRecovery.taskEvents) {
      await e.wait();
      e.clear();
    }
    expect(TestQueueRecovery.taskCount).toEqual(5);
    await expect(originalHandle.getResult()).resolves.toEqual([0, 1, 2, 3, 4]);

    // Recover the workflow, there should be one handle for the workflow and another for each task.
    await setWfAndChildrenToPending(wfid);
    const recoveryHandles = await recoverPendingWorkflows();
    for (const e of TestQueueRecovery.taskEvents) {
      await e.wait();
    }
    expect(recoveryHandles.length).toBe(TestQueueRecovery.queuedSteps + 1);

    // Verify both the recovered and original workflows complete correctly
    for (const h of recoveryHandles) {
      if (h.workflowID === wfid) {
        await expect(h.getResult()).resolves.toEqual([0, 1, 2, 3, 4]);
      }
    }

    await expect(originalHandle.getResult()).resolves.toEqual([0, 1, 2, 3, 4]);

    // Each task should start twice, once originally and once in recovery
    expect(TestQueueRecovery.taskCount).toEqual(10);

    // Verify all queue entries eventually get cleaned up
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });

  test('test-queue-concurrency-under-recovery', async () => {
    const wfid1 = randomUUID();
    const wfh1 = await DBOS.startWorkflow(TestQueueRecovery, {
      workflowID: wfid1,
      queueName: TestQueueRecovery.recoveryQueue.name,
    }).blockedWorkflow(0);
    const wfid2 = randomUUID();
    const wfh2 = await DBOS.startWorkflow(TestQueueRecovery, {
      workflowID: wfid2,
      queueName: TestQueueRecovery.recoveryQueue.name,
    }).blockedWorkflow(1);
    const wfid3 = randomUUID();
    const wfh3 = await DBOS.startWorkflow(TestWFs, {
      workflowID: wfid3,
      queueName: TestQueueRecovery.recoveryQueue.name,
    }).noop();

    for (const e of TestQueueRecovery.startEvents) {
      await e.wait();
      e.clear();
    }
    expect(TestQueueRecovery.cnt).toBe(2);

    const workflows = await DBOS.listQueuedWorkflows({ queueName: TestQueueRecovery.recoveryQueue.name });
    expect(workflows.length).toBe(3);
    expect(workflows[0].workflowID).toBe(wfid1);
    expect(workflows[0].executorId).toBe('local');
    expect((await wfh1.getStatus())?.status).toBe(StatusString.PENDING);
    expect(workflows[1].workflowID).toBe(wfid2);
    expect(workflows[1].executorId).toBe('local');
    expect((await wfh2.getStatus())?.status).toBe(StatusString.PENDING);
    expect(workflows[2].workflowID).toBe(wfid3);
    expect(workflows[2].executorId).toBe('local');
    expect((await wfh3.getStatus())?.status).toBe(StatusString.ENQUEUED);

    // Manually update the database to pretend wf3 is PENDING and comes from a different executor
    const systemDBClient = new Client({
      connectionString: config.systemDatabaseUrl,
    });
    await systemDBClient.connect();
    try {
      await systemDBClient.query(
        "UPDATE dbos.workflow_status SET executor_id = 'test-vmid-2', status = 'PENDING' WHERE workflow_uuid = $1",
        [wfh3.workflowID],
      );

      // Trigger workflow recovery. The two first workflows should still be blocked but the 3rd one enqueued
      const recovered_handles = await recoverPendingWorkflows(['test-vmid-2']);
      expect(recovered_handles.length).toBe(1);
      expect(recovered_handles[0].workflowID).toBe(wfid3);
      expect((await wfh1.getStatus())?.status).toBe(StatusString.PENDING);
      expect((await wfh2.getStatus())?.status).toBe(StatusString.PENDING);
      expect((await wfh3.getStatus())?.status).toBe(StatusString.ENQUEUED);

      // Unblock the two first workflows
      TestQueueRecovery.stopEvent.set();
      // Verify all queue entries eventually get cleaned up.
      expect(await wfh1.getResult()).toBeUndefined();
      expect(await wfh2.getResult()).toBeUndefined();
      expect(await wfh3.getResult()).toBeUndefined();
      expect(TestQueueRecovery.cnt).toBe(2);

      // Trigger workflow recovery for "local", by changing the record to indicate they did not finish.
      //   The two first workflows should be re-enqueued then dequeued again
      await setWfAndChildrenToPending(wfh1.workflowID);
      await setWfAndChildrenToPending(wfh2.workflowID);
      const recovered_handles_local = await recoverPendingWorkflows(['local']);
      expect(recovered_handles_local.length).toBe(2);
      for (const h of recovered_handles_local) {
        expect([wfid1, wfid2]).toContain(h.workflowID);
      }
      expect(await wfh1.getResult()).toBeUndefined();
      expect(await wfh2.getResult()).toBeUndefined();
      expect(TestQueueRecovery.cnt).toBe(4);

      const result = await systemDBClient.query(
        'SELECT executor_id FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfh3.workflowID],
      );
      expect(result.rows).toEqual([{ executor_id: 'local' }]);
      expect(await queueEntriesAreCleanedUp()).toBe(true);
    } finally {
      await systemDBClient.end();
    }
  });

  class TestCancelQueues {
    static startEvent = new Event();
    static blockingEvent = new Event();
    static queue: QueueRef = { name: 'TestCancelQueues', config: { concurrency: 1, ...testPolling } };

    @DBOS.workflow()
    static async stuckWorkflow() {
      TestCancelQueues.startEvent.set();
      await TestCancelQueues.blockingEvent.wait();
    }

    @DBOS.workflow()
    static async regularWorkflow() {
      return Promise.resolve();
    }
  }

  test('test-cancel-queues', async () => {
    const wfid = randomUUID();

    // Enqueue the blocked and regular workflow on a queue with concurrency 1
    const blockedHandle = await DBOS.startWorkflow(TestCancelQueues, {
      workflowID: wfid,
      queueName: TestCancelQueues.queue.name,
    }).stuckWorkflow();
    const regularHandle = await DBOS.startWorkflow(TestCancelQueues, {
      queueName: TestCancelQueues.queue.name,
    }).regularWorkflow();

    // Verify the blocked workflow starts and is PENDING while the regular workflow remains ENQUEUED
    await TestCancelQueues.startEvent.wait();
    await expect(blockedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
    });
    await expect(regularHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.ENQUEUED,
    });

    // Cancel the blocked workflow. Verify the regular workflow runs.
    await DBOS.cancelWorkflow(wfid);
    await expect(blockedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
    await expect(regularHandle.getResult()).resolves.toBeUndefined();

    // Complete the blocked workflow
    TestCancelQueues.blockingEvent.set();
    await expect(blockedHandle.getResult()).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);

    // Verify all queue entries eventually get cleaned up
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });

  class TestResumeQueues {
    static startEvent = new Event();
    static blockingEvent = new Event();
    static queue: QueueRef = { name: 'TestResumeQueues', config: { concurrency: 1, ...testPolling } };

    @DBOS.workflow()
    static async stuckWorkflow() {
      TestResumeQueues.startEvent.set();
      await TestResumeQueues.blockingEvent.wait();
    }

    @DBOS.workflow()
    static async regularWorkflow() {
      return Promise.resolve();
    }
  }

  test('test-resume-queues', async () => {
    const wfid = randomUUID();

    // Enqueue the blocked and regular workflow on a queue with concurrency 1
    const blockedHandle = await DBOS.startWorkflow(TestResumeQueues, {
      queueName: TestResumeQueues.queue.name,
    }).stuckWorkflow();
    const regularHandle = await DBOS.startWorkflow(TestResumeQueues, {
      workflowID: wfid,
      queueName: TestResumeQueues.queue.name,
    }).regularWorkflow();
    const regularHandleTwo = await DBOS.startWorkflow(TestResumeQueues, {
      queueName: TestResumeQueues.queue.name,
    }).regularWorkflow();

    // Verify the blocked workflow starts and is PENDING while the regular workflows remain ENQUEUED
    await TestResumeQueues.startEvent.wait();
    await expect(blockedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
    });
    await expect(regularHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.ENQUEUED,
    });
    await expect(regularHandleTwo.getStatus()).resolves.toMatchObject({
      status: StatusString.ENQUEUED,
    });

    await DBOS.resumeWorkflow(wfid);

    await expect(regularHandle.getResult()).resolves.toBeUndefined();

    // Complete the blocked workflow. Verify the second regular workflow also completes.
    TestResumeQueues.blockingEvent.set();
    await expect(blockedHandle.getResult()).resolves.toBeUndefined();
    await expect(regularHandleTwo.getResult()).resolves.toBeUndefined();

    // Verify all queue entries eventually get cleaned up
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });

  class TestResumeQueuesPartitioned {
    static startEvent = new Event();
    static blockingEvent = new Event();
    static queue: QueueRef = {
      name: 'TestResumeQueuesPartitioned',
      config: { concurrency: 1, partitionQueue: true, ...testPolling },
    };

    @DBOS.workflow()
    static async stuckWorkflow() {
      TestResumeQueuesPartitioned.startEvent.set();
      await TestResumeQueuesPartitioned.blockingEvent.wait();
    }

    @DBOS.workflow()
    static async regularWorkflow() {
      return Promise.resolve();
    }
  }

  test('test-resume-partitioned-queues', async () => {
    const wfid = randomUUID();
    const key = 'key';

    // Enqueue the blocked and regular workflow on a queue with concurrency 1
    const blockedHandle = await DBOS.startWorkflow(TestResumeQueuesPartitioned, {
      queueName: TestResumeQueuesPartitioned.queue.name,
      enqueueOptions: { queuePartitionKey: key },
    }).stuckWorkflow();
    const regularHandle = await DBOS.startWorkflow(TestResumeQueuesPartitioned, {
      workflowID: wfid,
      queueName: TestResumeQueuesPartitioned.queue.name,
      enqueueOptions: { queuePartitionKey: key },
    }).regularWorkflow();
    const regularHandleTwo = await DBOS.startWorkflow(TestResumeQueuesPartitioned, {
      queueName: TestResumeQueuesPartitioned.queue.name,
      enqueueOptions: { queuePartitionKey: key },
    }).regularWorkflow();

    // Verify the blocked workflow starts and is PENDING while the regular workflows remain ENQUEUED
    await TestResumeQueuesPartitioned.startEvent.wait();
    await expect(blockedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
    });
    await expect(regularHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.ENQUEUED,
    });
    await expect(regularHandleTwo.getStatus()).resolves.toMatchObject({
      status: StatusString.ENQUEUED,
    });

    await DBOS.resumeWorkflow(wfid);
    console.log('RESUMED', wfid);

    await expect(regularHandle.getResult()).resolves.toBeUndefined();

    // Complete the blocked workflow. Verify the second regular workflow also completes.
    TestResumeQueuesPartitioned.blockingEvent.set();
    await expect(blockedHandle.getResult()).resolves.toBeUndefined();
    await expect(regularHandleTwo.getResult()).resolves.toBeUndefined();

    // Verify all queue entries eventually get cleaned up
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });

  class TestConcurrencyAcrossVersions {
    static queue: QueueRef = {
      name: 'TestAcrossVersions',
      config: { workerConcurrency: 1, ...testPolling },
    };

    @DBOS.workflow()
    static async testWorkflow() {
      return Promise.resolve(DBOS.workflowID);
    }
  }

  test('test-concurrency-across-versions', async () => {
    expect(config.systemDatabaseUrl).toBeDefined();
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });

    const other_version = 'other_version';
    const other_version_handle = await client.enqueue({
      queueName: TestConcurrencyAcrossVersions.queue.name,
      workflowName: 'testWorkflow',
      workflowClassName: 'TestConcurrencyAcrossVersions',
      appVersion: other_version,
    });

    const handle = await DBOS.startWorkflow(TestConcurrencyAcrossVersions, {
      queueName: TestConcurrencyAcrossVersions.queue.name,
    }).testWorkflow();
    await expect(handle.getResult()).resolves.toBeTruthy();

    globalParams.appVersion = other_version;
    await expect(other_version_handle.getResult()).resolves.toBeTruthy();
    await client.destroy();
  });

  test('test_wait_first_queue', async () => {
    const numTasks = WaitFirstQueueTest.numTasks;
    WaitFirstQueueTest.resetEvents();
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(WaitFirstQueueTest, { workflowID: wfid }).processTasks();

    // Release tasks in reverse order, waiting for each to be consumed
    for (let roundIdx = 0; roundIdx < numTasks; roundIdx++) {
      const taskId = numTasks - 1 - roundIdx;
      WaitFirstQueueTest.goResolvers[taskId]();
      await WaitFirstQueueTest.consumedPromises[roundIdx];
    }

    const result = await handle.getResult();
    const expected = Array.from({ length: numTasks }, (_, i) => `result-${numTasks - 1 - i}`);
    expect(result).toEqual(expected);

    // Verify the steps are correct:
    // 5 enqueue steps + 5 (waitFirst, getResult) pairs = 15 steps
    const steps = await DBOS.listWorkflowSteps(wfid);
    expect(steps).toBeDefined();
    expect(steps!.length).toBe(numTasks * 3);

    // First numTasks steps are the enqueues (functionID is 0-based)
    for (let i = 0; i < numTasks; i++) {
      expect(steps![i].functionID).toBe(i);
      expect(steps![i].name).toBe('processTask');
      expect(steps![i].childWorkflowID).not.toBeNull();
    }

    // Remaining steps alternate between waitFirst and getResult
    for (let i = 0; i < numTasks; i++) {
      const waitStep = steps![numTasks + i * 2];
      const resultStep = steps![numTasks + i * 2 + 1];
      expect(waitStep.name).toBe('DBOS.waitFirst');
      expect(resultStep.name).toBe('DBOS.getResult');
    }

    // Fork from the last waitFirst step and verify same result
    const lastWaitFirstStepId = steps![numTasks + (numTasks - 1) * 2].functionID;
    const forkedHandle = await DBOS.forkWorkflow(wfid, lastWaitFirstStepId);
    expect(await forkedHandle.getResult()).toEqual(expected);

    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });
});

const waitFirstQueue: QueueRef = { name: 'wait_first_queue', config: { concurrency: 5, ...testPolling } };

class WaitFirstQueueTest {
  static numTasks = 5;
  static goResolvers: (() => void)[] = [];
  static goPromises: Promise<void>[] = [];
  static consumedResolvers: (() => void)[] = [];
  static consumedPromises: Promise<void>[] = [];

  static resetEvents() {
    WaitFirstQueueTest.goResolvers = [];
    WaitFirstQueueTest.goPromises = [];
    WaitFirstQueueTest.consumedResolvers = [];
    WaitFirstQueueTest.consumedPromises = [];
    for (let i = 0; i < WaitFirstQueueTest.numTasks; i++) {
      let goResolve!: () => void;
      WaitFirstQueueTest.goPromises.push(new Promise<void>((r) => (goResolve = r)));
      WaitFirstQueueTest.goResolvers.push(goResolve);
      let consumedResolve!: () => void;
      WaitFirstQueueTest.consumedPromises.push(new Promise<void>((r) => (consumedResolve = r)));
      WaitFirstQueueTest.consumedResolvers.push(consumedResolve);
    }
  }

  @DBOS.workflow()
  static async processTask(taskId: number) {
    await WaitFirstQueueTest.goPromises[taskId];
    return `result-${taskId}`;
  }

  @DBOS.workflow()
  static async processTasks() {
    const handles: WorkflowHandle<string>[] = [];
    for (let i = 0; i < WaitFirstQueueTest.numTasks; i++) {
      const handle = await DBOS.startWorkflow(WaitFirstQueueTest, { queueName: waitFirstQueue.name }).processTask(i);
      handles.push(handle);
    }

    const results: string[] = [];
    let remaining = [...handles];
    for (let roundIdx = 0; roundIdx < WaitFirstQueueTest.numTasks; roundIdx++) {
      const completed = await DBOS.waitFirst(remaining);
      const result = (await completed.getResult()) as string;
      results.push(result);
      remaining = remaining.filter((h) => h.workflowID !== completed.workflowID);
      WaitFirstQueueTest.consumedResolvers[roundIdx]();
    }
    return results;
  }
}

// Dummy declaration to match the workflow in tests/wfqueueworker.ts
export class InterProcessWorkflowTask {
  @DBOS.workflow()
  static async task(_: number) {
    return Promise.resolve();
  }
}

// Single DB-backed queue shared between the parent process and the worker
// child processes. The parent does not listen on it (see `listenQueues: []`
// in the test); only the workers dequeue. Concurrency is filled in from
// `InterProcessWorkflow`'s class constants when the queue is registered.
const IPWQueueName = 'IPWQueue';

class InterProcessWorkflow {
  static localConcurrencyLimit = 5;
  static globalConcurrencyLimit = InterProcessWorkflow.localConcurrencyLimit * 2;

  @DBOS.workflow()
  static async testGlobalConcurrency(systemDatabaseUrl: string) {
    // First, start local concurrency limit tasks
    let handles = [];
    for (let i = 0; i < InterProcessWorkflow.localConcurrencyLimit; ++i) {
      handles.push(await DBOS.startWorkflow(InterProcessWorkflowTask, { queueName: IPWQueueName }).task(i));
    }
    // Start two workers
    const workerPromises = await InterProcessWorkflow.startWorkerProcesses(2);
    try {
      // Check that a single worker was able to acquire all the tasks
      let n_dequeued = 0;
      while (n_dequeued < InterProcessWorkflow.localConcurrencyLimit) {
        const msg = await DBOS.recv<string>('worker_dequeue', 1);
        if (msg === 'worker_dequeue') {
          n_dequeued++;
        }
      }
      let executors = [];
      for (const handle of handles) {
        const status = await handle.getStatus();
        expect(status).not.toBeNull();
        expect(status?.status).toBe(StatusString.PENDING);
        executors.push(status?.executorId);
      }
      expect(new Set(executors).size).toBe(1);

      // Now enqueue less than the local concurrency limit. Check that the 2nd worker acquired them.
      handles = [];
      for (let i = 0; i < InterProcessWorkflow.localConcurrencyLimit - 1; ++i) {
        handles.push(await DBOS.startWorkflow(InterProcessWorkflowTask, { queueName: IPWQueueName }).task(i));
      }
      n_dequeued = 0;
      while (n_dequeued < InterProcessWorkflow.localConcurrencyLimit - 1) {
        const msg = await DBOS.recv<string>('worker_dequeue', 1);
        if (msg === 'worker_dequeue') {
          n_dequeued++;
        }
      }

      executors = [];
      for (const handle of handles) {
        const status = await handle.getStatus();
        expect(status).not.toBeNull();
        expect(status?.status).toBe(StatusString.PENDING);
        executors.push(status?.executorId);
      }
      expect(new Set(executors).size).toBe(1);

      // Now, enqueue two more tasks. This means qlen > local concurrency limit * 2 and qlen > global concurrency limit
      // We should have 1 tasks PENDING and 1 ENQUEUED, thus meeting both local and global concurrency limits
      handles = [];
      for (let i = 0; i < 2; ++i) {
        handles.push(
          await DBOS.startWorkflow(InterProcessWorkflowTask, { queueName: IPWQueueName }).task(
            InterProcessWorkflow.localConcurrencyLimit - 1 + i,
          ),
        );
      }
      // The first worker already sent a signal. Here we are waiting for the second worker.
      n_dequeued = 0;
      while (n_dequeued < 1) {
        const msg = await DBOS.recv<string>('worker_dequeue', 1);
        if (msg === 'worker_dequeue') {
          n_dequeued++;
        }
      }
      executors = [];
      const statuses = [];
      for (const handle of handles) {
        const status = await handle.getStatus();
        expect(status).not.toBeNull();
        statuses.push(status?.status);
        executors.push(status?.executorId);
      }
      expect(statuses).toContain(StatusString.PENDING);
      expect(statuses).toContain(StatusString.ENQUEUED);
      expect(new Set(executors).size).toBe(2);
      expect(executors).toContain('local');

      // Now check the global concurrency is met
      const systemDBClient = new Client({
        connectionString: systemDatabaseUrl,
      });
      await systemDBClient.connect();
      try {
        const result = await systemDBClient.query<{ count: string }>(
          'SELECT COUNT(*) FROM dbos.workflow_status WHERE status = $1 AND queue_name = $2',
          [StatusString.PENDING, IPWQueueName],
        );
        const count = Number(result.rows[0].count);
        expect(count).toBe(InterProcessWorkflow.globalConcurrencyLimit);
      } finally {
        await systemDBClient.end();
      }

      // Notify the workers they can resume
      await DBOS.setEvent('worker_resume', true);
    } finally {
      await Promise.all(workerPromises);
    }
  }

  static startWorkerProcesses(nWorkers: number): Promise<Promise<void>[]> {
    const workerPromises: Promise<void>[] = [];
    for (let i = 0; i < nWorkers; i++) {
      const workerId = `test-worker-${i}`;
      const workerPromise = new Promise<void>((resolve, reject) => {
        const child = spawn(
          'npx',
          [
            'ts-node',
            './tests/wfqueueworker.ts',
            `${InterProcessWorkflow.localConcurrencyLimit}`,
            `${InterProcessWorkflow.globalConcurrencyLimit}`,
            `${DBOS.workflowID}`,
            `${IPWQueueName}`,
          ],
          {
            cwd: process.cwd(),
            env: { ...process.env, DBOS__VMID: workerId, DBOS__APPVERSION: globalParams.appVersion },
            stdio: 'inherit', // Allows direct streaming
          },
        );

        child.on('close', (code) => {
          if (code === 0) {
            resolve();
          } else {
            reject(new Error(`Worker ${i} exited with code ${code}`));
          }
        });

        child.on('error', (error) => {
          console.error(`Worker ${i} failed: ${error.message}`);
          reject(error);
        });
      });
      workerPromises.push(workerPromise);
    }
    return Promise.resolve(workerPromises);
  }
}

describe('queued-wf-tests-concurrent-workers', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
  });

  beforeEach(async () => {
    // The parent does not dispatch IPWQueue — only the spawned workers do.
    // listenQueues: [] disables every user queue on this process.
    DBOS.setConfig({ ...config, listenQueues: [] });
    await DBOS.launch();
    await DBOS.registerQueue(IPWQueueName, {
      onConflict: 'always_update',
      workerConcurrency: InterProcessWorkflow.localConcurrencyLimit,
      concurrency: InterProcessWorkflow.globalConcurrencyLimit,
      ...testPolling,
    });
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('test_global_and_local_concurrency', async () => {
    expect(config.systemDatabaseUrl).toBeDefined();
    const wfh = await DBOS.startWorkflow(InterProcessWorkflow).testGlobalConcurrency(config.systemDatabaseUrl!);
    await wfh.getResult();
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  }, 60000);
});

class TestWFs {
  static wfCounter = 0;
  static stepCounter = 0;
  static wfid: string;

  static reset() {
    TestWFs.wfCounter = 0;
    TestWFs.stepCounter = 0;
  }

  @DBOS.workflow()
  static async testWorkflow(var1: string, var2: string) {
    expect(DBOS.workflowID).toBe(TestWFs.wfid);
    ++TestWFs.wfCounter;
    var1 = await TestWFs.testStep(var1);
    return Promise.resolve(var1 + var2);
  }

  @DBOS.workflow()
  static async testWorkflowSimple(var1: string, var2: string) {
    ++TestWFs.wfCounter;
    return Promise.resolve(var1 + var2);
  }

  @DBOS.step()
  static async testStep(str: string) {
    ++TestWFs.stepCounter;
    return Promise.resolve(str + 'd');
  }

  @DBOS.workflow()
  static async testWorkflowTime(var1: string, var2: string): Promise<number> {
    expect(var1).toBe('abc');
    expect(var2).toBe('123');
    return Promise.resolve(Date.now());
  }

  @DBOS.workflow()
  static async noop() {
    return Promise.resolve();
  }
}

class TestWFs2 {
  static wfCounter = 0;
  static flag = false;
  static wfid: string;
  static mainResolve?: () => void;
  static wfPromise?: Promise<void>;

  static reset() {
    TestWFs2.wfCounter = 0;
    TestWFs2.flag = false;
  }

  @DBOS.workflow()
  static async workflowOne() {
    ++TestWFs2.wfCounter;
    TestWFs2.mainResolve?.();
    await TestWFs2.wfPromise;
    return Promise.resolve();
  }

  @DBOS.workflow()
  static async workflowTwo() {
    TestWFs2.flag = true; // Tell if this ran yet
    return Promise.resolve();
  }
}

class TestChildWFs {
  @DBOS.workflow()
  static async testWorkflow(var1: string, var2: string) {
    const wfh1 = await DBOS.startWorkflow(TestChildWFs, { queueName: childqueue.name }).testChildWF(var1);
    const wfh2 = await DBOS.startWorkflow(TestChildWFs, { queueName: childqueue.name }).testChildWF(var2);
    const wfh3 = await DBOS.startWorkflow(TestChildWFs, { queueName: childqueue.name }).testChildWF(var1);
    const wfh4 = await DBOS.startWorkflow(TestChildWFs, { queueName: childqueue.name }).testChildWF(var2);

    await DBOS.sleepms(1000);
    expect((await wfh4.getStatus())?.status).toBe(StatusString.ENQUEUED);

    await DBOS.send(wfh1.workflowID, 'go', 'release');
    await DBOS.send(wfh2.workflowID, 'go', 'release');
    await DBOS.send(wfh3.workflowID, 'go', 'release');
    await DBOS.send(wfh4.workflowID, 'go', 'release');

    return (await wfh1.getResult()) + (await wfh2.getResult()) + (await wfh3.getResult()) + (await wfh4.getResult());
  }

  @DBOS.workflow()
  static async testChildWF(str: string) {
    await DBOS.recv('release', 30);
    return Promise.resolve(str + 'd');
  }
}

async function runOneAtATime(queue: QueueRef) {
  let wfRes: () => void = () => {};
  TestWFs2.wfPromise = new Promise<void>((resolve, _rj) => {
    wfRes = resolve;
  });
  const mainPromise = new Promise<void>((resolve, _rj) => {
    TestWFs2.mainResolve = resolve;
  });
  const wfh1 = await DBOS.startWorkflow(TestWFs2, { queueName: queue.name }).workflowOne();
  expect((await wfh1.getStatus())?.queueName).toBe(queue.name);
  const wfh2 = await DBOS.startWorkflow(TestWFs2, { queueName: queue.name }).workflowTwo();
  expect((await wfh2.getStatus())?.queueName).toBe(queue.name);
  await mainPromise;
  await sleepms(2000);
  expect(TestWFs2.flag).toBeFalsy();
  wfRes?.();
  await wfh1.getResult();
  await wfh2.getResult();
  expect(TestWFs2.flag).toBeTruthy();
  expect(TestWFs2.wfCounter).toBe(1);
  expect(await queueEntriesAreCleanedUp()).toBe(true);
}

describe('enqueue-options', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    // childqueue is shared with the simple block; the others are local.
    for (const ref of [
      childqueue,
      TestExample.queue,
      TestExample.queue2,
      TestPriority.queue,
      TestPriority.childqueue,
      SetPriorityTest.setPriorityQueue,
    ]) {
      await DBOS.registerQueue(ref.name, { onConflict: 'always_update', ...ref.config });
    }
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  class TestExample {
    static resolveEvent: () => void;
    static workflowEvent = new Promise<void>((resolve) => {
      TestExample.resolveEvent = resolve;
    });

    static queue: QueueRef = { name: 'test_dedup_queue', config: { concurrency: 1, ...testPolling } };
    static queue2: QueueRef = { name: 'queue2', config: { concurrency: 1, ...testPolling } };

    @DBOS.workflow()
    static async parentWorkflow(input: string): Promise<string> {
      const wfh1 = await DBOS.startWorkflow(TestExample, { queueName: childqueue.name }).childWorkflow(input);

      await TestExample.workflowEvent;

      const result = await wfh1.getResult();
      return Promise.resolve(result + '-p');
    }

    @DBOS.workflow()
    static async childWorkflow(input: string): Promise<string> {
      await TestExample.workflowEvent;
      return Promise.resolve(input + '-c');
    }
  }

  test('test_deduplication', async () => {
    const wfid = randomUUID();
    const dedupID = 'my_dedup_id';

    const wfh1 = await DBOS.startWorkflow(TestExample, {
      workflowID: wfid,
      queueName: TestExample.queue.name,
      enqueueOptions: { deduplicationID: dedupID },
    }).parentWorkflow('abc');
    const status = await wfh1.getStatus();
    assert.equal(status?.deduplicationID, dedupID);

    // different dup_id no issue
    const wfid2 = randomUUID();

    const wfh2 = await DBOS.startWorkflow(TestExample, {
      workflowID: wfid2,
      queueName: TestExample.queue.name,
      enqueueOptions: { deduplicationID: 'my_dedup_id2' },
    }).parentWorkflow('ghi');

    // no dedupid fine
    const wfid3 = randomUUID();
    const wfh3 = await DBOS.startWorkflow(TestExample, {
      workflowID: wfid3,
      queueName: TestExample.queue.name,
    }).parentWorkflow('jk1');

    // same dedup id, but different workflowID
    const wfid4 = randomUUID();

    let expectedError = false;
    try {
      await DBOS.startWorkflow(TestExample, {
        workflowID: wfid4,
        queueName: TestExample.queue.name,
        enqueueOptions: { deduplicationID: dedupID },
      }).parentWorkflow('xyz');
    } catch (err) {
      expectedError = true;
      expect(err).toBeInstanceOf(DBOSQueueDuplicatedError);
    }
    expect(expectedError).toBe(true);

    // same dedup id, but different queue
    const wfid5 = randomUUID();
    const wfh4 = await DBOS.startWorkflow(TestExample, {
      workflowID: wfid5,
      queueName: TestExample.queue2.name,
      enqueueOptions: { deduplicationID: dedupID },
    }).parentWorkflow('xyz');
    expect((await wfh4.getStatus())?.status).toBe(StatusString.ENQUEUED);

    TestExample.resolveEvent();

    expect(wfh1).toBeDefined();

    const result1 = await wfh1.getResult();
    expect(result1).toBe('abc-c-p');

    expect(wfh2).toBeDefined();
    const result2 = await wfh2.getResult();

    expect(result2).toBe('ghi-c-p');

    const result3 = await wfh3.getResult();
    expect(result3).toBe('jk1-c-p');

    const result4 = await wfh4.getResult();
    expect(result4).toBe('xyz-c-p');
  });

  class TestPriority {
    static resolveEvent: () => void;
    static workflowEvent = new Promise<void>((resolve) => {
      TestPriority.resolveEvent = resolve;
    });

    static wfPriorityList: number[] = [];

    static queue: QueueRef = {
      name: 'test_queue_prority',
      config: { concurrency: 1, priorityEnabled: true, ...testPolling },
    };
    static childqueue: QueueRef = {
      name: 'child_queue',
      config: { concurrency: 1, priorityEnabled: true, ...testPolling },
    };

    @DBOS.workflow()
    static async parentWorkflow(input: number): Promise<number> {
      // 0 means no priority
      TestPriority.wfPriorityList.push(input);

      const wfh1 = await DBOS.startWorkflow(TestPriority, {
        queueName: TestPriority.childqueue.name,
        enqueueOptions: input !== 0 ? { priority: input } : undefined,
      }).childWorkflow(input);

      await TestPriority.workflowEvent;

      const result = await wfh1.getResult();
      return Promise.resolve(input + result);
    }

    @DBOS.workflow()
    static async childWorkflow(priority: number): Promise<number> {
      await TestPriority.workflowEvent;
      return Promise.resolve(priority);
    }
  }

  test('test_priorityqueue', async () => {
    const wf_handles = [];

    const handle = await DBOS.startWorkflow(TestPriority, { queueName: TestPriority.queue.name }).parentWorkflow(0);

    wf_handles.push(handle);

    for (let i = 1; i <= 5; i++) {
      const handle = await DBOS.startWorkflow(TestPriority, {
        queueName: TestPriority.queue.name,
        enqueueOptions: { priority: i },
      }).parentWorkflow(i);
      const status = await handle.getStatus();
      assert.equal(status?.priority, i);
      wf_handles.push(handle);
    }

    wf_handles.push(await DBOS.startWorkflow(TestPriority, { queueName: TestPriority.queue.name }).parentWorkflow(6));
    wf_handles.push(await DBOS.startWorkflow(TestPriority, { queueName: TestPriority.queue.name }).parentWorkflow(7));

    TestPriority.resolveEvent();

    for (let i = 0; i < wf_handles.length; i++) {
      const res = await wf_handles[i].getResult();
      expect(res).toBe(i * 2);
    }

    expect(TestPriority.wfPriorityList).toEqual([0, 6, 7, 1, 2, 3, 4, 5]);

    // test invalid priority

    await expect(
      DBOS.startWorkflow(TestPriority, {
        queueName: TestPriority.queue.name,
        enqueueOptions: { priority: DBOS_QUEUE_MIN_PRIORITY - 10 },
      }).parentWorkflow(7),
    ).rejects.toBeInstanceOf(DBOSInvalidQueuePriorityError);

    await expect(
      DBOS.startWorkflow(TestPriority, {
        queueName: TestPriority.queue.name,
        enqueueOptions: { priority: DBOS_QUEUE_MAX_PRIORITY + 1 },
      }).parentWorkflow(7),
    ).rejects.toBeInstanceOf(DBOSInvalidQueuePriorityError);
  });

  class SetPriorityTest {
    static setPriorityQueue: QueueRef = {
      name: 'test_set_priority_queue',
      config: { concurrency: 1, priorityEnabled: true, ...testPolling },
    };

    static resolveBlocker: () => void;
    static blockerPromise = new Promise<void>((resolve) => {
      SetPriorityTest.resolveBlocker = resolve;
    });

    static executionOrder: number[] = [];

    @DBOS.workflow()
    static async blockerWorkflow(): Promise<void> {
      await SetPriorityTest.blockerPromise;
    }

    @DBOS.workflow()
    static async trackedWorkflow(id: number): Promise<number> {
      await Promise.resolve();
      SetPriorityTest.executionOrder.push(id);
      return id;
    }
  }

  test('test_setWorkflowPriority', async () => {
    // Start a blocker workflow to hold the queue
    const blockerHandle = await DBOS.startWorkflow(SetPriorityTest, {
      queueName: SetPriorityTest.setPriorityQueue.name,
    }).blockerWorkflow();

    // Enqueue workflows with priorities 5, 4, 3
    const handle1 = await DBOS.startWorkflow(SetPriorityTest, {
      queueName: SetPriorityTest.setPriorityQueue.name,
      enqueueOptions: { priority: 5 },
    }).trackedWorkflow(1);

    const handle2 = await DBOS.startWorkflow(SetPriorityTest, {
      queueName: SetPriorityTest.setPriorityQueue.name,
      enqueueOptions: { priority: 4 },
    }).trackedWorkflow(2);

    const handle3 = await DBOS.startWorkflow(SetPriorityTest, {
      queueName: SetPriorityTest.setPriorityQueue.name,
      enqueueOptions: { priority: 3 },
    }).trackedWorkflow(3);

    // Update priority of workflow 1 (was 5) to 1 (highest)
    await DBOS.setWorkflowPriority(handle1.workflowID, 1);

    // Verify the priority was updated
    const status = await handle1.getStatus();
    expect(status?.priority).toBe(1);

    // Unblock the queue
    SetPriorityTest.resolveBlocker();
    await blockerHandle.getResult();

    // Wait for all to complete
    await handle1.getResult();
    await handle2.getResult();
    await handle3.getResult();

    // Workflow 1 should run first (priority 1), then 3 (priority 3), then 2 (priority 4)
    expect(SetPriorityTest.executionOrder).toEqual([1, 3, 2]);

    // Test invalid priority
    await expect(DBOS.setWorkflowPriority('some-id', 0)).rejects.toBeInstanceOf(DBOSInvalidQueuePriorityError);
    await expect(DBOS.setWorkflowPriority('some-id', -1)).rejects.toBeInstanceOf(DBOSInvalidQueuePriorityError);
    await expect(DBOS.setWorkflowPriority('some-id', DBOS_QUEUE_MAX_PRIORITY + 1)).rejects.toBeInstanceOf(
      DBOSInvalidQueuePriorityError,
    );
  });
});

// Timeout tests rely on the 1s default polling delay for timing assumptions
const timeoutQueue: QueueRef = { name: 'timeout-test-queue', config: {} };

describe('queue-time-outs', () => {
  let config: DBOSConfig;

  const events_map = new Map<string, Event>();
  class DBOSTimeoutTestClass {
    @DBOS.workflow()
    static async sleepingWorkflow(duration: number) {
      await DBOS.sleep(duration);
      const workflowID = DBOS.workflowID as string;
      const event = events_map.get(workflowID);
      if (event) {
        event.set();
      }
      return 42;
    }

    @DBOS.workflow()
    static async blockedWorkflow() {
      const workflowID = DBOS.workflowID as string;
      const event = events_map.get(workflowID);
      if (event) {
        event.set();
      }
      while (true) {
        await DBOS.sleep(100);
      }
    }

    @DBOS.workflow()
    static async timeoutParentStartWF(timeout: number) {
      await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: timeout })
        .blockedWorkflow()
        .then((h) => h.getResult());
    }

    @DBOS.workflow()
    static async timeoutParentEnqueueWF(timeout: number) {
      await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: timeout, queueName: timeoutQueue.name })
        .blockedWorkflow()
        .then((h) => h.getResult());
    }

    @DBOS.workflow()
    static async timeoutParentStartDetachedChild(duration: number) {
      await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: null })
        .sleepingWorkflow(duration * 2)
        .then((h) => h.getResult());
    }

    @DBOS.workflow()
    static async timeoutParentStartDetachedChildWithSyntax(duration: number) {
      await DBOS.withWorkflowTimeout(null, async () => {
        await DBOSTimeoutTestClass.sleepingWorkflow(duration * 2);
      });
    }

    @DBOS.workflow()
    static async timeoutParentEnqueueDetached(duration: number) {
      await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: null, queueName: timeoutQueue.name })
        .sleepingWorkflow(duration * 2)
        .then((h) => h.getResult());
    }
  }

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    for (const ref of [timeoutQueue, dedupRecoveryQueue, partitionQueue]) {
      await DBOS.registerQueue(ref.name, { onConflict: 'always_update', ...ref.config });
    }
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  // enqueue workflow with timeout
  test('enqueue-workflow-withWorkflowTimeout', async () => {
    const workflowID: string = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: timeoutQueue.name,
      timeoutMS: 100,
    }).blockedWorkflow();
    await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(workflowID));
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.CANCELLED);
    expect(status?.timeoutMS).toBe(100);
  });

  // enqueue workflow with *no* timeout which calls a blocked child with timeout
  test('enqueue-workflow-withChildTimeout', async () => {
    const workflowID: string = randomUUID();
    const childID: string = `${workflowID}-0`;
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: timeoutQueue.name,
    }).timeoutParentStartWF(100);
    await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(childID));
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.ERROR,
    });
    const childHandle = DBOS.retrieveWorkflow(childID);
    await expect(childHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
  });

  // enqueue workflow with timeout which enqueues a blocked child. Parent times out *before* the child. Both are cancelled
  test('enqueue-workflow-withEnqueueChildTimeout', async () => {
    const workflowID: string = randomUUID();
    const childID: string = `${workflowID}-0`;
    events_map.set(childID, new Event());
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: timeoutQueue.name,
      timeoutMS: 1000,
    }).timeoutParentEnqueueWF(100); // The trick here is that the child deadline starts at dequeue, which happens after the 1s dequeue polling interval
    await events_map.get(childID)?.wait();
    await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(workflowID));

    // Because the deadline is set when we dequeue, there is actually some
    const childHandle = DBOS.retrieveWorkflow(childID);
    await expect(childHandle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(childID));

    const statuses = await DBOS.listWorkflows({ workflow_id_prefix: workflowID });
    expect(statuses.length).toBe(2);
    statuses.forEach((status) => {
      expect(status.status).toBe(StatusString.CANCELLED);
    });
  });

  // enqueue workflow with timeout which enqueues a blocked child. Parent times out *after* the child. Only the child is cancelled and the parent errors
  test('enqueue-workflow-withEnqueueChildTimeout', async () => {
    const workflowID: string = randomUUID();
    const childID: string = `${workflowID}-0`;
    events_map.set(childID, new Event());
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: timeoutQueue.name,
      timeoutMS: 2000, // allow a dequeue interval to pass
    }).timeoutParentEnqueueWF(100);
    await events_map.get(childID)?.wait();
    await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(childID));
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.ERROR,
    });

    const childHandle = DBOS.retrieveWorkflow(childID);
    await expect(childHandle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(childID));
    await expect(childHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
  });

  // enqueue a parent workflow with timeout which directly calls a detached child
  test('enqueue-workflow-withDetachedChildTimeout', async () => {
    const workflowID: string = randomUUID();
    const childID: string = `${workflowID}-0`;
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: timeoutQueue.name,
      timeoutMS: 100,
    }).timeoutParentStartDetachedChild(100);
    await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(workflowID));
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
    const childHandle = DBOS.retrieveWorkflow(childID);
    await expect(childHandle.getResult()).resolves.toBe(42);
    await expect(childHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  // enqueue a parent workflow with timeout which directly calls a detached child (with withWorkflowTimeout syntax)
  test('enqueue-workflow-withDetachedChildTimeoutWithSyntax', async () => {
    const workflowID: string = randomUUID();
    const childID: string = `${workflowID}-0`;
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: timeoutQueue.name,
      timeoutMS: 100,
    }).timeoutParentStartDetachedChildWithSyntax(100);
    await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(workflowID));
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
    const childHandle = DBOS.retrieveWorkflow(childID);
    await expect(childHandle.getResult()).resolves.toBe(42);
    await expect(childHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  // enqueue parent workflow with timeout which enqueues a detached child
  test('enqueue-workflow-withDetachedChildTimeoutEnqueue', async () => {
    const workflowID: string = randomUUID();
    const childID: string = `${workflowID}-0`;
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: timeoutQueue.name,
      timeoutMS: 100,
    }).timeoutParentEnqueueDetached(100);
    await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(workflowID));
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
    const childHandle = DBOS.retrieveWorkflow(childID);
    await expect(childHandle.getResult()).resolves.toBe(42);
    await expect(childHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  // enqueued workflow w/ recovery gets proper deadline
  test('enqueue-workflow-with-workflowTimeout-recovery', async () => {
    const workflowID: string = randomUUID();
    events_map.set(workflowID, new Event());
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: timeoutQueue.name,
      timeoutMS: 3000,
    }).blockedWorkflow();
    await events_map.get(workflowID)?.wait();
    const status = await handle.getStatus();

    // Trigger recovery. Deadline should be the same
    const recoveryHandles = await recoverPendingWorkflows(['local']);
    expect(recoveryHandles.length).toBe(1);
    const recoveryStatus = await recoveryHandles[0].getStatus();
    expect(recoveryStatus?.timeoutMS).toBe(3000);
    expect(status?.deadlineEpochMS).toBe(recoveryStatus?.deadlineEpochMS);

    await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(workflowID));
    await expect(recoveryHandles[0].getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(workflowID));
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
  });

  const dedupRecoveryEvent = new Event();
  const dedupRecoveryQueue: QueueRef = { name: 'dedup-recovery-queue', config: { ...testPolling } };
  const dedupRecoveryKey = 'my-dedup-id';
  const dedupRecoveryParentWorkflow = DBOS.registerWorkflow(
    async () => {
      const handle = await DBOS.startWorkflow(dedupRecoveryChildWorkflow, {
        queueName: dedupRecoveryQueue.name,
        enqueueOptions: { deduplicationID: dedupRecoveryKey },
      })();
      await assert.rejects(async () => {
        await DBOS.startWorkflow(dedupRecoveryChildWorkflow, {
          queueName: dedupRecoveryQueue.name,
          enqueueOptions: { deduplicationID: dedupRecoveryKey },
        })();
      }, Error);
      return handle.workflowID;
    },
    { name: 'dedupRecoveryParentWorkflow' },
  );

  const dedupRecoveryChildWorkflow = DBOS.registerWorkflow(
    async () => {
      await dedupRecoveryEvent.wait();
    },
    { name: 'dedupRecoveryChildWorkflow' },
  );

  test('dedup-recovery', async () => {
    const handle = await DBOS.startWorkflow(dedupRecoveryParentWorkflow)();
    const childID = await handle.getResult();
    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    assert.ok(steps !== undefined);
    assert.equal(steps.length, 2);
    // Instanceof is not preserved by serialization, so we must assert the error code
    assert.ok(steps[1].error !== null);
    assert.equal(getDBOSErrorCode(steps[1].error), QueueDedupIDDuplicated);
    dedupRecoveryEvent.set();
    const childHandle = DBOS.retrieveWorkflow(childID);
    await childHandle.getResult();

    // Verify it still works on recovery
    const recoveredHandle = await reexecuteWorkflowById(handle.workflowID);
    await recoveredHandle!.getResult();
  });

  const partitionBlockingEvent = new Event();
  const partitionWaitingEvent = new Event();
  const partitionQueue: QueueRef = {
    name: 'partition-queue',
    config: { partitionQueue: true, concurrency: 1, ...testPolling },
  };

  const partitionBlockedWorkflow = DBOS.registerWorkflow(
    async () => {
      partitionWaitingEvent.set();
      await partitionBlockingEvent.wait();
      return DBOS.workflowID;
    },
    { name: 'partitionBlockedWorkflow' },
  );

  const partitionNormalWorkflow = DBOS.registerWorkflow(
    async () => {
      return Promise.resolve(DBOS.workflowID);
    },
    { name: 'partitionNormalWorkflow' },
  );

  test('partitioned-queues', async () => {
    const blockedPartitionKey = 'blocked';
    const normalPartitionKey = 'normal';

    // Enqueue a blocked workflow and a normal workflow on the blocked partition
    // Verify the blocked workflow starts but the normal workflow is stuck behind it
    const blockedBlockedHandle = await DBOS.startWorkflow(partitionBlockedWorkflow, {
      queueName: partitionQueue.name,
      enqueueOptions: { queuePartitionKey: blockedPartitionKey },
    })();
    const status = await blockedBlockedHandle.getStatus();
    assert.equal(status?.queuePartitionKey, blockedPartitionKey);
    const blockedNormalHandle = await DBOS.startWorkflow(partitionNormalWorkflow, {
      queueName: partitionQueue.name,
      enqueueOptions: { queuePartitionKey: blockedPartitionKey },
    })();

    await partitionWaitingEvent.wait();
    await expect(blockedBlockedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
    });
    await expect(blockedNormalHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.ENQUEUED,
    });

    // Enqueue a normal workflow on the other partition and verify it runs normally
    const normalHandle = await DBOS.startWorkflow(partitionNormalWorkflow, {
      queueName: partitionQueue.name,
      enqueueOptions: { queuePartitionKey: normalPartitionKey },
    })();
    await normalHandle.getResult();

    // Unblock the blocked partition and verify its workflows complete
    partitionBlockingEvent.set();
    await blockedBlockedHandle.getResult();
    await blockedNormalHandle.getResult();

    // Confirm client enqueue works with partitions
    expect(config.systemDatabaseUrl).toBeDefined();
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const clientHandle = await client.enqueue({
        queueName: partitionQueue.name,
        workflowName: 'partitionNormalWorkflow',
        queuePartitionKey: blockedPartitionKey,
      });
      await clientHandle.getResult();
    } finally {
      await client.destroy();
    }

    // Deduplication is not supported for partitioned queues. This check is
    // purely from supplied params, so it fires regardless of whether the
    // queue is in the in-memory map.
    await assert.rejects(async () => {
      await DBOS.startWorkflow(partitionNormalWorkflow, {
        queueName: partitionQueue.name,
        enqueueOptions: { queuePartitionKey: normalPartitionKey, deduplicationID: 'key' },
      })();
    }, Error);
  });

  test('explicit-queue-listen-test', async () => {
    await DBOS.shutdown();
    const config = generateDBOSTestConfig();
    // Reset the test database
    await setUpDBOSTestSysDb(config);

    const workflow = DBOS.registerWorkflow(
      async () => {
        return Promise.resolve(DBOS.workflowID);
      },
      { name: 'explicit-queue-listen-test' },
    );

    config.listenQueues = ['queue-one'];
    DBOS.setConfig(config);
    await DBOS.launch();

    // Both queues are persisted in the database; only the one in
    // listenQueues will have a worker on this process.
    await DBOS.registerQueue('queue-one', { onConflict: 'always_update', ...testPolling });
    await DBOS.registerQueue('queue-two', { onConflict: 'always_update', ...testPolling });

    const handleOne = await DBOS.startWorkflow(workflow, { queueName: 'queue-one' })();
    const handleTwo = await DBOS.startWorkflow(workflow, { queueName: 'queue-two' })();
    assert.equal(await handleOne.getResult(), handleOne.workflowID);
    const status = await handleTwo.getStatus();
    assert.equal(status?.status, 'ENQUEUED');

    await DBOS.shutdown();
    config.listenQueues = ['queue-one', 'queue-two'];
    DBOS.setConfig(config);
    await DBOS.launch();

    const retrievedHandle = DBOS.retrieveWorkflow(handleTwo.workflowID);
    assert.equal(await retrievedHandle.getResult(), retrievedHandle.workflowID);
    const forkedHandle = await DBOS.forkWorkflow(handleTwo.workflowID, 0);
    assert.equal(await forkedHandle.getResult(), forkedHandle.workflowID);
  });
});

describe('delay-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.registerQueue(TestDelayWFs.queue.name, {
      onConflict: 'always_update',
      ...TestDelayWFs.queue.config,
    });
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  class TestDelayWFs {
    static readonly queue: QueueRef = { name: 'delay-test-queue', config: { ...testPolling } };

    @DBOS.workflow()
    static async testWorkflow(): Promise<void> {
      await Promise.resolve();
    }

    @DBOS.workflow()
    static async testWorkflowStr(): Promise<string> {
      return await Promise.resolve('done');
    }
  }

  test('test_delay', async () => {
    const delaySeconds = 1.5;

    // Test via startWorkflow with enqueueOptions
    const tBefore = Date.now();
    const handle = await DBOS.startWorkflow(TestDelayWFs, {
      queueName: TestDelayWFs.queue.name,
      enqueueOptions: { delaySeconds },
    }).testWorkflow();
    const tAfter = Date.now();

    const status = await handle.getStatus();
    expect(status?.status).toBe(StatusString.DELAYED);
    expect(status?.delayUntilEpochMS).toBeDefined();
    expect(status!.delayUntilEpochMS!).toBeGreaterThanOrEqual(tBefore + delaySeconds * 1000);
    expect(status!.delayUntilEpochMS!).toBeLessThanOrEqual(tAfter + delaySeconds * 1000);

    await handle.getResult();

    const finalStatus = await handle.getStatus();
    expect(finalStatus?.status).toBe(StatusString.SUCCESS);
    expect(finalStatus?.dequeuedAt).toBeDefined();
    expect(finalStatus!.dequeuedAt!).toBeGreaterThanOrEqual(status!.delayUntilEpochMS!);

    // Test via client enqueue
    expect(config.systemDatabaseUrl).toBeDefined();
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const tClientBefore = Date.now();
      const clientHandle = await client.enqueue({
        queueName: TestDelayWFs.queue.name,
        workflowName: 'testWorkflow',
        workflowClassName: 'TestDelayWFs',
        delaySeconds,
      });
      const tClientAfter = Date.now();

      const clientStatus = await clientHandle.getStatus();
      expect(clientStatus?.status).toBe(StatusString.DELAYED);
      expect(clientStatus?.delayUntilEpochMS).toBeDefined();
      expect(clientStatus!.delayUntilEpochMS!).toBeGreaterThanOrEqual(tClientBefore + delaySeconds * 1000);
      expect(clientStatus!.delayUntilEpochMS!).toBeLessThanOrEqual(tClientAfter + delaySeconds * 1000);

      await clientHandle.getResult();

      const finalClientStatus = await clientHandle.getStatus();
      expect(finalClientStatus?.status).toBe(StatusString.SUCCESS);
      expect(finalClientStatus?.dequeuedAt).toBeDefined();
      expect(finalClientStatus!.dequeuedAt!).toBeGreaterThanOrEqual(clientStatus!.delayUntilEpochMS!);
    } finally {
      await client.destroy();
    }

    // Delayed workflows appear in listWorkflows and listQueuedWorkflows
    const listHandle = await DBOS.startWorkflow(TestDelayWFs, {
      queueName: TestDelayWFs.queue.name,
      enqueueOptions: { delaySeconds: 60 },
    }).testWorkflow();
    const allWorkflows = await DBOS.listWorkflows({ status: StatusString.DELAYED });
    expect(allWorkflows.some((w) => w.workflowID === listHandle.workflowID)).toBe(true);
    const queuedWorkflows = await DBOS.listQueuedWorkflows({ queueName: TestDelayWFs.queue.name });
    expect(queuedWorkflows.some((w) => w.workflowID === listHandle.workflowID)).toBe(true);

    // waitFirst treats DELAYED as active and unblocks when it completes
    const waitHandle = await DBOS.startWorkflow(TestDelayWFs, {
      queueName: TestDelayWFs.queue.name,
      enqueueOptions: { delaySeconds: 1 },
    }).testWorkflow();
    expect((await waitHandle.getStatus())?.status).toBe(StatusString.DELAYED);
    const completed = await DBOS.waitFirst([waitHandle]);
    expect(completed.workflowID).toBe(waitHandle.workflowID);
    expect((await completed.getStatus())?.status).toBe(StatusString.SUCCESS);

    // Deduplication: a second enqueue with the same dedup ID should fail while DELAYED
    const dedupID = randomUUID();
    const dedupHandle = await DBOS.startWorkflow(TestDelayWFs, {
      queueName: TestDelayWFs.queue.name,
      enqueueOptions: { delaySeconds: 60, deduplicationID: dedupID },
    }).testWorkflow();
    expect((await dedupHandle.getStatus())?.status).toBe(StatusString.DELAYED);
    await expect(
      DBOS.startWorkflow(TestDelayWFs, {
        queueName: TestDelayWFs.queue.name,
        enqueueOptions: { delaySeconds: 60, deduplicationID: dedupID },
      }).testWorkflow(),
    ).rejects.toBeInstanceOf(DBOSQueueDuplicatedError);
  });

  test('test_delay_cancel_resume', async () => {
    // Cancel a DELAYED workflow — it should never run
    const cancelHandle = await DBOS.startWorkflow(TestDelayWFs, {
      queueName: TestDelayWFs.queue.name,
      enqueueOptions: { delaySeconds: 60 },
    }).testWorkflowStr();
    expect((await cancelHandle.getStatus())?.status).toBe(StatusString.DELAYED);
    await DBOS.cancelWorkflow(cancelHandle.workflowID);
    expect((await cancelHandle.getStatus())?.status).toBe(StatusString.CANCELLED);

    // Verify it does not appear in queued workflows after cancellation
    const queued = await DBOS.listQueuedWorkflows({ queueName: TestDelayWFs.queue.name });
    expect(queued.some((w) => w.workflowID === cancelHandle.workflowID)).toBe(false);

    // Resume a DELAYED workflow — it should run immediately, bypassing the delay
    const resumeHandle = await DBOS.startWorkflow(TestDelayWFs, {
      queueName: TestDelayWFs.queue.name,
      enqueueOptions: { delaySeconds: 60 },
    }).testWorkflowStr();
    expect((await resumeHandle.getStatus())?.status).toBe(StatusString.DELAYED);
    await DBOS.resumeWorkflow(resumeHandle.workflowID);
    expect(await resumeHandle.getResult()).toBe('done');
    expect((await resumeHandle.getStatus())?.status).toBe(StatusString.SUCCESS);
  });

  test('test_setWorkflowDelay', async () => {
    // Start a workflow with a long delay
    const handle = await DBOS.startWorkflow(TestDelayWFs, {
      queueName: TestDelayWFs.queue.name,
      enqueueOptions: { delaySeconds: 60 },
    }).testWorkflowStr();
    expect((await handle.getStatus())?.status).toBe(StatusString.DELAYED);

    // Update the delay to a short value
    const tBefore = Date.now();
    await DBOS.setWorkflowDelay(handle.workflowID, 1);
    const tAfter = Date.now();

    // Verify the delay was updated
    const status = await handle.getStatus();
    expect(status?.status).toBe(StatusString.DELAYED);
    expect(status?.delayUntilEpochMS).toBeDefined();
    expect(status!.delayUntilEpochMS!).toBeGreaterThanOrEqual(tBefore + 1000);
    expect(status!.delayUntilEpochMS!).toBeLessThanOrEqual(tAfter + 1000);

    // Wait for the workflow to complete
    expect(await handle.getResult()).toBe('done');
    expect((await handle.getStatus())?.status).toBe(StatusString.SUCCESS);

    // Test invalid delay
    await expect(DBOS.setWorkflowDelay('some-id', -1)).rejects.toThrow('delaySeconds must be greater than 0');
  });

  test('test_setWorkflowDelay_options', async () => {
    // Test with delaySeconds option
    const handle1 = await DBOS.startWorkflow(TestDelayWFs, {
      queueName: TestDelayWFs.queue.name,
      enqueueOptions: { delaySeconds: 60 },
    }).testWorkflowStr();
    expect((await handle1.getStatus())?.status).toBe(StatusString.DELAYED);

    const tBefore1 = Date.now();
    await DBOS.setWorkflowDelay(handle1.workflowID, { delaySeconds: 1 });
    const tAfter1 = Date.now();

    const status1 = await handle1.getStatus();
    expect(status1?.status).toBe(StatusString.DELAYED);
    expect(status1!.delayUntilEpochMS!).toBeGreaterThanOrEqual(tBefore1 + 1000);
    expect(status1!.delayUntilEpochMS!).toBeLessThanOrEqual(tAfter1 + 1000);
    expect(await handle1.getResult()).toBe('done');

    // Test with delayUntilEpochMS option
    const handle2 = await DBOS.startWorkflow(TestDelayWFs, {
      queueName: TestDelayWFs.queue.name,
      enqueueOptions: { delaySeconds: 60 },
    }).testWorkflowStr();
    expect((await handle2.getStatus())?.status).toBe(StatusString.DELAYED);

    const deadline = Date.now() + 1000;
    await DBOS.setWorkflowDelay(handle2.workflowID, { delayUntilEpochMS: deadline });

    const status2 = await handle2.getStatus();
    expect(status2?.status).toBe(StatusString.DELAYED);
    expect(status2!.delayUntilEpochMS).toBe(deadline);
    expect(await handle2.getResult()).toBe('done');
  });
});

describe('database-backed-queue-crud', () => {
  beforeAll(async () => {
    await setUpDBOSTestSysDb(generateDBOSTestConfig());
  });

  beforeEach(async () => {
    // Reset the config every test so a prior test's listenQueues filter
    // doesn't leak across the suite.
    DBOS.setConfig(generateDBOSTestConfig());
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('register-retrieve-delete-and-conflict-resolution', async () => {
    const queueName = `test_crud_queue_${randomUUID()}`;

    expect(await DBOS.retrieveQueue(queueName)).toBeNull();

    const registered = await DBOS.registerQueue(queueName, {
      concurrency: 10,
      rateLimit: { limitPerPeriod: 5, periodSec: 1.5 },
      workerConcurrency: 2,
      priorityEnabled: true,
      minPollingIntervalMs: 2500,
    });
    expect(registered.name).toBe(queueName);
    expect(registered.databaseBacked).toBe(true);
    // Database-backed queues are not added to the in-memory registry.
    expect(wfQueueRunner.wfQueuesByName.has(queueName)).toBe(false);

    let retrieved = await DBOS.retrieveQueue(queueName);
    expect(retrieved).not.toBeNull();
    expect(retrieved!.name).toBe(queueName);
    expect(retrieved!.concurrency).toBe(10);
    expect(retrieved!.workerConcurrency).toBe(2);
    expect(retrieved!.rateLimit).toEqual({ limitPerPeriod: 5, periodSec: 1.5 });
    expect(retrieved!.priorityEnabled).toBe(true);
    expect(retrieved!.partitionQueue).toBe(false);
    expect(retrieved!.minPollingIntervalMs).toBe(2500);
    expect(retrieved!.databaseBacked).toBe(true);

    // never_update leaves the existing row alone.
    await DBOS.registerQueue(queueName, { concurrency: 99, onConflict: 'never_update' });
    retrieved = await DBOS.retrieveQueue(queueName);
    expect(retrieved!.concurrency).toBe(10);
    expect(retrieved!.rateLimit).toEqual({ limitPerPeriod: 5, periodSec: 1.5 });

    // always_update overwrites every column, including clearing those that
    // were previously set but are now omitted.
    await DBOS.registerQueue(queueName, { concurrency: 20, onConflict: 'always_update' });
    retrieved = await DBOS.retrieveQueue(queueName);
    expect(retrieved!.concurrency).toBe(20);
    expect(retrieved!.workerConcurrency).toBeUndefined();
    expect(retrieved!.rateLimit).toBeUndefined();
    expect(retrieved!.priorityEnabled).toBe(false);
    expect(retrieved!.minPollingIntervalMs).toBe(1000);

    // update_if_latest_version updates when the running version is the latest.
    await DBOS.registerQueue(queueName, { concurrency: 30, onConflict: 'update_if_latest_version' });
    retrieved = await DBOS.retrieveQueue(queueName);
    expect(retrieved!.concurrency).toBe(30);

    // If a newer registered version exists, update_if_latest_version no-ops.
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const newerVersion = `newer-${randomUUID()}`;
    await sysdb.createApplicationVersion(newerVersion);
    await sysdb.updateApplicationVersionTimestamp(newerVersion, Date.now() + 1_000_000);

    await DBOS.registerQueue(queueName, { concurrency: 999, onConflict: 'update_if_latest_version' });
    retrieved = await DBOS.retrieveQueue(queueName);
    expect(retrieved!.concurrency).toBe(30);

    // delete removes the row; subsequent retrievals return null and a
    // second delete is a harmless no-op.
    await DBOS.deleteQueue(queueName);
    expect(await DBOS.retrieveQueue(queueName)).toBeNull();
    await DBOS.deleteQueue(queueName);
  });

  test('dynamic-config-via-setters', async () => {
    const queueName = `test_dyn_queue_${randomUUID()}`;
    const queue = await DBOS.registerQueue(queueName, {
      concurrency: 4,
      workerConcurrency: 2,
      priorityEnabled: false,
      minPollingIntervalMs: 1000,
    });

    await queue.setConcurrency(8);
    await queue.setWorkerConcurrency(3);
    await queue.setRateLimit({ limitPerPeriod: 7, periodSec: 2.0 });
    await queue.setPriorityEnabled(true);
    await queue.setPartitionQueue(true);
    await queue.setMinPollingIntervalMs(500);

    const fresh = await DBOS.retrieveQueue(queueName);
    for (const q of [queue, fresh]) {
      expect(q).not.toBeNull();
      expect(q!.concurrency).toBe(8);
      expect(q!.workerConcurrency).toBe(3);
      expect(q!.rateLimit).toEqual({ limitPerPeriod: 7, periodSec: 2.0 });
      expect(q!.priorityEnabled).toBe(true);
      expect(q!.partitionQueue).toBe(true);
      expect(q!.minPollingIntervalMs).toBe(500);
    }

    // Cross-validation: workerConcurrency cannot exceed concurrency.
    await expect(queue.setWorkerConcurrency(100)).rejects.toThrow(
      'workerConcurrency must be less than or equal to concurrency',
    );
    // Polling interval must be positive.
    await expect(queue.setMinPollingIntervalMs(0)).rejects.toThrow('minPollingIntervalMs must be positive');

    // Rate limit can be cleared.
    await queue.setRateLimit(undefined);
    const cleared = await DBOS.retrieveQueue(queueName);
    expect(cleared!.rateLimit).toBeUndefined();

    // In-memory queues do not support setters.
    const legacyName = `legacy_dyn_queue_${randomUUID()}`;
    const legacy = new WorkflowQueue(legacyName, { concurrency: 2 });
    expect(legacy.concurrency).toBe(2);
    expect(legacy.databaseBacked).toBe(false);
    await expect(legacy.setConcurrency(5)).rejects.toThrow(/dynamic configuration is only supported/);

    await DBOS.deleteQueue(queueName);
  });

  test('supervisor-launches-worker-for-queue-registered-after-launch', async () => {
    const queueName = `supervisor_${randomUUID()}`;
    // Queue is registered AFTER DBOS launches; the supervisor's discovery
    // cycle must pick it up and start a worker without a process restart.
    await DBOS.registerQueue(queueName, { minPollingIntervalMs: 100 });

    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestWFs, { workflowID: wfid, queueName }).testWorkflowSimple('hi', '!');
    expect(await handle.getResult()).toBe('hi!');

    await DBOS.deleteQueue(queueName);
  });

  test('worker-exits-and-resumes-on-delete-and-recreate', async () => {
    const queueName = `recreate_${randomUUID()}`;
    await DBOS.registerQueue(queueName, { minPollingIntervalMs: 100 });

    // The original worker processes one workflow.
    const h1 = await DBOS.startWorkflow(TestWFs, { workflowID: randomUUID(), queueName }).testWorkflowSimple('a', '1');
    expect(await h1.getResult()).toBe('a1');

    // Deleting the row makes the worker exit on its next iteration.
    await DBOS.deleteQueue(queueName);
    expect(await DBOS.retrieveQueue(queueName)).toBeNull();

    // Re-registering with new config; the supervisor must start a fresh
    // worker for the new row.
    await sleepms(3000);
    await DBOS.registerQueue(queueName, { minPollingIntervalMs: 100, concurrency: 5 });
    expect((await DBOS.retrieveQueue(queueName))!.concurrency).toBe(5);

    const h2 = await DBOS.startWorkflow(TestWFs, { workflowID: randomUUID(), queueName }).testWorkflowSimple('b', '2');
    expect(await h2.getResult()).toBe('b2');

    await DBOS.deleteQueue(queueName);
  });

  test('listenQueues-mixed-instances-and-strings', async () => {
    // beforeEach already launched DBOS with the default config; reset it so
    // we can launch with a custom listenQueues filter.
    await DBOS.shutdown();

    const inMemQueueName = `inmem_${randomUUID()}`;
    const allowedDbName = `allowed_db_${randomUUID()}`;
    const filteredOutDbName = `filtered_db_${randomUUID()}`;

    const inMemQueue = new WorkflowQueue(inMemQueueName, { minPollingIntervalMs: 100 });

    const cfg = generateDBOSTestConfig();
    cfg.listenQueues = [inMemQueue, allowedDbName];
    DBOS.setConfig(cfg);
    await DBOS.launch();

    // Register both DB-backed queues. The supervisor must launch a worker
    // for the allowed name and skip the filtered one.
    await DBOS.registerQueue(allowedDbName, { minPollingIntervalMs: 100 });
    await DBOS.registerQueue(filteredOutDbName, { minPollingIntervalMs: 100 });

    // The in-memory queue (passed as an instance) processes its workflow.
    const h1 = await DBOS.startWorkflow(TestWFs, { queueName: inMemQueueName }).testWorkflowSimple('a', '1');
    expect(await h1.getResult()).toBe('a1');

    // The DB-backed queue (passed as a string name) also processes.
    const h2 = await DBOS.startWorkflow(TestWFs, { queueName: allowedDbName }).testWorkflowSimple('b', '2');
    expect(await h2.getResult()).toBe('b2');

    // The filtered-out DB-backed queue gets no worker; its workflow stays
    // ENQUEUED. Wait long enough for several supervisor cycles to confirm
    // the filter holds.
    const h3 = await DBOS.startWorkflow(TestWFs, { queueName: filteredOutDbName }).testWorkflowSimple('c', '3');
    await sleepms(2500);
    expect((await h3.getStatus())?.status).toBe(StatusString.ENQUEUED);

    await DBOS.deleteQueue(allowedDbName);
    await DBOS.deleteQueue(filteredOutDbName);
  });

  test('async-getters-reflect-cross-process-changes', async () => {
    const queueName = `test_freshness_${randomUUID()}`;
    await DBOS.registerQueue(queueName, { concurrency: 5 });

    // Two independent handles to the same row simulate two processes each
    // holding their own snapshot.
    const q1 = await DBOS.retrieveQueue(queueName);
    const q2 = await DBOS.retrieveQueue(queueName);
    expect(q1!.concurrency).toBe(5);
    expect(q2!.concurrency).toBe(5);

    // q2 mutates the row; q1's local cache is now stale.
    await q2!.setConcurrency(10);
    expect(q1!.concurrency).toBe(5);

    // Async getter on q1 re-reads from the DB and refreshes the cache.
    expect(await q1!.getConcurrency()).toBe(10);
    expect(q1!.concurrency).toBe(10);

    // A single async getter call refreshes every cached field in one query.
    await q2!.setRateLimit({ limitPerPeriod: 3, periodSec: 1 });
    await q2!.setPriorityEnabled(true);
    expect(q1!.rateLimit).toBeUndefined();
    expect(q1!.priorityEnabled).toBe(false);
    await q1!.getConcurrency();
    expect(q1!.rateLimit).toEqual({ limitPerPeriod: 3, periodSec: 1 });
    expect(q1!.priorityEnabled).toBe(true);

    // Async getters throw when the row is gone.
    await DBOS.deleteQueue(queueName);
    await expect(q1!.getConcurrency()).rejects.toThrow(/not found in the database/);

    // In-memory queues: getters return cached values without a DB roundtrip.
    const memName = `test_mem_freshness_${randomUUID()}`;
    const mem = new WorkflowQueue(memName, { concurrency: 7, priorityEnabled: true });
    expect(await mem.getConcurrency()).toBe(7);
    expect(await mem.getPriorityEnabled()).toBe(true);
  });

  test('in-memory-and-db-backed-queues-coexist', async () => {
    // beforeEach already launched DBOS with the default config; restart it
    // with a custom listenQueues filter so we can exercise mixed listening.
    await DBOS.shutdown();

    const listenedMemName = `inmem_listened_${randomUUID()}`;
    const idleMemName = `inmem_idle_${randomUUID()}`;
    const dbBackedName = `dbbacked_${randomUUID()}`;

    const listenedMem = new WorkflowQueue(listenedMemName, { minPollingIntervalMs: 100 });
    new WorkflowQueue(idleMemName, { concurrency: 2, minPollingIntervalMs: 100 });

    // Re-declaring an in-memory queue with the same name throws.
    expect(() => new WorkflowQueue(listenedMemName)).toThrow(/defined multiple times/);

    // listenQueues accepts a mix of WorkflowQueue instances and string names.
    const cfg = generateDBOSTestConfig();
    cfg.listenQueues = [listenedMem, dbBackedName];
    DBOS.setConfig(cfg);
    await DBOS.launch();

    // Register the database-backed queue post-launch; the supervisor picks it
    // up since it matches the listen filter.
    await DBOS.registerQueue(dbBackedName, { minPollingIntervalMs: 100 });

    // The listened in-memory queue runs workflows.
    const memHandle = await DBOS.startWorkflow(TestWFs, { queueName: listenedMemName }).testWorkflowSimple('a', '1');
    expect(await memHandle.getResult()).toBe('a1');

    // The listened database-backed queue runs workflows.
    const dbHandle = await DBOS.startWorkflow(TestWFs, { queueName: dbBackedName }).testWorkflowSimple('b', '2');
    expect(await dbHandle.getResult()).toBe('b2');

    // A workflow enqueued on the un-listened in-memory queue stays ENQUEUED.
    const idleHandle = await DBOS.startWorkflow(TestWFs, { queueName: idleMemName }).testWorkflowSimple('c', '3');
    await sleepms(2000);
    expect((await idleHandle.getStatus())?.status).toBe(StatusString.ENQUEUED);

    // Restart DBOS listening to the previously idle queue. The pending
    // workflow now runs to completion.
    await DBOS.shutdown();
    const cfg2 = generateDBOSTestConfig();
    cfg2.listenQueues = [idleMemName];
    DBOS.setConfig(cfg2);
    await DBOS.launch();

    const resumed = DBOS.retrieveWorkflow(idleHandle.workflowID);
    expect(await resumed.getResult()).toBe('c3');

    await DBOS.deleteQueue(dbBackedName);
  });

  class DynConcWFs {
    static startedCount = 0;
    static releaseEvent = new Event();
    static reset() {
      DynConcWFs.startedCount = 0;
      DynConcWFs.releaseEvent = new Event();
    }
    @DBOS.workflow()
    static async blocking(): Promise<void> {
      DynConcWFs.startedCount += 1;
      await DynConcWFs.releaseEvent.wait();
    }
  }

  test('dynamic-concurrency-takes-effect-on-running-worker', async () => {
    DynConcWFs.reset();
    const queueName = `dyn_conc_${randomUUID()}`;
    const queue = await DBOS.registerQueue(queueName, { concurrency: 1, minPollingIntervalMs: 100 });

    try {
      const handles = await Promise.all([0, 1, 2].map(() => DBOS.startWorkflow(DynConcWFs, { queueName }).blocking()));

      // With concurrency=1, exactly one workflow should start. Wait long
      // enough for several poll iterations to confirm the limit holds.
      const concDeadline = Date.now() + 5000;
      while (Date.now() < concDeadline && DynConcWFs.startedCount < 1) {
        await sleepms(50);
      }
      expect(DynConcWFs.startedCount).toBe(1);
      await sleepms(1000);
      expect(DynConcWFs.startedCount).toBe(1);

      // Bump concurrency. The worker reloads its config from the database on
      // its next poll iteration and starts the remaining two workflows.
      await queue.setConcurrency(3);

      const deadline = Date.now() + 5000;
      while (Date.now() < deadline && DynConcWFs.startedCount < 3) {
        await sleepms(50);
      }
      expect(DynConcWFs.startedCount).toBe(3);

      // Release everything; all three complete.
      DynConcWFs.releaseEvent.set();
      for (const h of handles) {
        await h.getResult();
      }
    } finally {
      DynConcWFs.releaseEvent.set();
      await DBOS.deleteQueue(queueName);
    }
  });
});

// Regression test for the "orphan-PENDING" bug in partitioned queue dispatch.
//
// Pre-fix: runQueue iterated partition keys, collected wfids from every
// findAndMarkStartableWorkflows call into a single array, and dispatched them
// after the loop. Each call committed in its own transaction, so the chosen
// workflow was already PENDING by the time the call returned. If a concurrent
// executor's FOR UPDATE NOWAIT raised 55P03 on a later partition, the outer
// catch cleared the array — the post-loop dispatch was skipped and the
// already-committed workflows were left orphaned in PENDING.
//
// Fix: dispatch each partition's workflows inline, immediately after they are
// marked PENDING.
describe('partitioned-queue-orphan-pending', () => {
  class PartitionWorkerWF {
    @DBOS.workflow()
    static async run(partitionKey: string): Promise<string> {
      await sleepms(150);
      return partitionKey;
    }
  }

  const STRESS_QUEUE_NAME = 'partition-orphan-stress-queue';

  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.registerQueue(STRESS_QUEUE_NAME, {
      partitionQueue: true,
      concurrency: 1,
      minPollingIntervalMs: 100,
      onConflict: 'always_update',
    });
  });

  afterEach(async () => {
    clearDebugTriggers();
    await DBOS.shutdown();
  });

  // Coordinate the race using two debug triggers:
  //   DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES — fires after each partition's
  //     dispatch. We sleep here so the test has time to arm the 55P03 trigger
  //     before the next partition's SELECT runs.
  //   DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT — fires inside
  //     findAndMarkStartableWorkflows between the SELECT FOR UPDATE NOWAIT and
  //     COMMIT. We throw a synthetic 55P03 here to simulate a concurrent
  //     executor winning the lock.
  //
  // Pre-fix: the first partition's workflow committed PENDING, the second
  // threw 55P03, the outer catch cleared wfids, the post-loop dispatch ran
  // with []  — that workflow was stuck PENDING forever.
  // Post-fix: the first partition's workflow is dispatched inline before the
  // second partition is even attempted, so the 55P03 cannot orphan it.
  test('partition-orphan-deterministic', async () => {
    const partitionA = `det-a-${randomUUID()}`;
    const partitionB = `det-b-${randomUUID()}`;

    // Phase 1: arm the between-partition pause so we can coordinate timing.
    // The trigger fires after partition A's transaction commits and the loop
    // is between partitions — wf-A is PENDING, wf-B is still ENQUEUED.
    let betweenFired = false;
    setDebugTrigger(DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES, {
      asyncCallback: async () => {
        betweenFired = true;
        await sleepms(500);
      },
    });

    const wfA = await DBOS.startWorkflow(PartitionWorkerWF, {
      queueName: STRESS_QUEUE_NAME,
      enqueueOptions: { queuePartitionKey: partitionA },
    }).run(partitionA);

    const wfB = await DBOS.startWorkflow(PartitionWorkerWF, {
      queueName: STRESS_QUEUE_NAME,
      enqueueOptions: { queuePartitionKey: partitionB },
    }).run(partitionB);

    // Wait for partition A to commit (between-partition trigger fires).
    const betweenDeadline = Date.now() + 5000;
    while (!betweenFired && Date.now() < betweenDeadline) {
      await sleepms(50);
    }
    expect(betweenFired).toBe(true);

    // Phase 2: while the loop is paused between A and B, arm the 55P03 trigger.
    // It fires once for partition B's SELECT — simulating a concurrent executor
    // winning the FOR UPDATE NOWAIT race on that row.
    let p03Fired = false;
    setDebugTrigger(DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT, {
      callback: () => {
        if (!p03Fired) {
          p03Fired = true;
          const err = new Error('could not obtain lock on row in relation "workflow_status"') as NodeJS.ErrnoException;
          err.code = '55P03';
          throw err;
        }
      },
    });
    // Replace the between-partition pause with a no-op so the loop can continue.
    setDebugTrigger(DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES, { callback: () => {} });

    // Phase 3: wait for wf-A (pre-fix: orphaned in PENDING; post-fix: SUCCESS).
    const deadline = Date.now() + 10000;
    let statusA = await wfA.getStatus();
    while (statusA?.status !== StatusString.SUCCESS && Date.now() < deadline) {
      await sleepms(100);
      statusA = await wfA.getStatus();
    }

    clearDebugTriggers();

    expect(p03Fired).toBe(true);
    expect(statusA?.status).toBe(StatusString.SUCCESS);
    expect(await wfA.getResult()).toBe(partitionA);

    // wf-B's partition threw 55P03 — retried on the next poll cycle.
    await expect(wfB.getResult()).resolves.toBe(partitionB);

    expect(await queueEntriesAreCleanedUp()).toBe(true);
  }, 20000);
});
