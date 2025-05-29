import { StatusString, WorkflowHandle, DBOS, ConfiguredInstance, DBOSClient } from '../src';
import {
  DBOSConfigInternal,
  DBOSExecutor,
  DBOS_QUEUE_MAX_PRIORITY,
  DBOS_QUEUE_MIN_PRIORITY,
} from '../src/dbos-executor';
import {
  generateDBOSTestConfig,
  setUpDBOSTestDb,
  Event,
  queueEntriesAreCleanedUp,
  recoverPendingWorkflows,
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
} from '../src/error';

const execFileAsync = promisify(execFile);

import {
  clearDebugTriggers,
  DEBUG_TRIGGER_WORKFLOW_QUEUE_START,
  // DEBUG_TRIGGER_WORKFLOW_ENQUEUE,
  setDebugTrigger,
} from '../src/debugpoint';

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

describe('queued-wf-tests-simple', () => {
  let config: DBOSConfigInternal;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    TestWFs.reset();
    TestWFs2.reset();
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

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
  }, 10000);

  test('child-wfs-queue', async () => {
    expect(await TestChildWFs.testWorkflow('a', 'b')).toBe('adbdadbd');
  }, 10000);

  test('test_one_at_a_time_with_limiter', async () => {
    await runOneAtATime(serialqueueLimited);
  }, 10000);

  test('test_one_at_a_time_with_worker_concurrency', async () => {
    await runOneAtATime(workerConcurrencyQueue);
  }, 10000);

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
        expect(times[i + 1] - times[i]).toBeLessThan(100);
      }
    }

    // Verify that the gap between "waves" is ~equal to the period
    for (let wave = 1; wave < numWaves; ++wave) {
      expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeGreaterThan(qperiod * 1000 - 300);
      expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeLessThan(qperiod * 1000 + 500);
    }

    for (const h of handles) {
      expect((await h.getStatus())!.status).toBe(StatusString.SUCCESS);
    }
  }, 10000);

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
        expect(times[i + 1] - times[i]).toBeLessThan(150);
      }
    }

    // Verify that the gap between "waves" is ~equal to the period
    for (let wave = 1; wave < numWaves; ++wave) {
      expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeGreaterThan(qperiod * 1000 - 300);
      expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeLessThan(qperiod * 1000 + 500);
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
  }, 10000);

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
  }, 10000);

  /*
    // Current result: WF1 does get created in system DB, but never starts running.
    //  WF2 does run.
    test("test_one_at_a_time_with_crash2", async() => {
        let wfqRes: () => void = () => { };
        const _wfqPromise = new Promise<void>((resolve, _rj) => { wfqRes = resolve; });

        setDebugTrigger(DEBUG_TRIGGER_WORKFLOW_ENQUEUE, {
            callback: () => {
                wfqRes();
                throw new Error("Interrupt start workflow here");
            }
        });

        const wfid1 = 'thisworkflowgetshit';
        console.log("Start WF1");
        try {
            const _wfh1 = await testRuntime.startWorkflow(TestWFs, {workflowID: wfid1, queueName: serialqueue.name}).testWorkflowSimple('a','b');
        }
        catch(e) {
            // Expected
            const err = e as Error;
            expect(err.message.includes('Interrupt')).toBeTruthy();
            console.log("Expected error caught");
        }
        console.log("Destroy runtime");
        await testRuntime.destroy();
        clearDebugTriggers();
        console.log("New runtime");
        testRuntime = await createInternalTestRuntime(undefined, config);
        console.log("Start WF2");
        const wfh2 = await DBOS.startWorkflow(TestWFs, {queueName: serialqueue.name}).testWorkflowSimple('c','d');

        const wfh1b = testRuntime.retrieveWorkflow(wfid1);
        const wfh2b = testRuntime.retrieveWorkflow(wfh2.workflowID);
        console.log("Wait");
        expect (await wfh2b.getResult()).toBe('cd');
        // Current behavior (undesired) WF1 got created but will stay ENQUEUED and not get run.
        expect((await wfh1b.getStatus())?.status).toBe('SUCCESS');
        expect (await wfh1b.getResult()).toBe('ab');
    }, 10000);
    */

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
    static event = new Event();
    static taskEvents = Array.from({ length: TestQueueRecovery.queuedSteps }, () => new Event());
    static taskCount = 0;
    static queue = new WorkflowQueue('testQueueRecovery');

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
      await TestQueueRecovery.event.wait();
      return i;
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

    // Recover the workflow, then resume it. There should be one handle for the workflow and another for each task.
    const recoveryHandles = await recoverPendingWorkflows();
    for (const e of TestQueueRecovery.taskEvents) {
      await e.wait();
    }
    expect(recoveryHandles.length).toBe(TestQueueRecovery.queuedSteps + 1);
    TestQueueRecovery.event.set();

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
    const recoveryQueue = new WorkflowQueue('recoveryQ', { concurrency: 2 });
    const wfid1 = randomUUID();
    const wfh1 = await DBOS.startWorkflow(TestQueueRecovery, {
      workflowID: wfid1,
      queueName: recoveryQueue.name,
    }).blockedWorkflow(0);
    const wfid2 = randomUUID();
    const wfh2 = await DBOS.startWorkflow(TestQueueRecovery, {
      workflowID: wfid2,
      queueName: recoveryQueue.name,
    }).blockedWorkflow(1);
    const wfid3 = randomUUID();
    const wfh3 = await DBOS.startWorkflow(TestWFs, {
      workflowID: wfid3,
      queueName: recoveryQueue.name,
    }).noop();

    for (const e of TestQueueRecovery.startEvents) {
      await e.wait();
      e.clear();
    }
    expect(TestQueueRecovery.cnt).toBe(2);

    const workflows = await DBOS.listQueuedWorkflows({ queueName: recoveryQueue.name });
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
      user: config.poolConfig.user,
      port: config.poolConfig.port,
      host: config.poolConfig.host,
      password: config.poolConfig.password,
      database: config.system_database,
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

      // Trigger workflow recovery for "local". The two first workflows should be re-enqueued then dequeued again
      const recovered_handles_local = await recoverPendingWorkflows(['local']);
      expect(recovered_handles_local.length).toBe(2);
      for (const h of recovered_handles_local) {
        expect([wfid1, wfid2]).toContain(h.workflowID);
      }
      for (const e of TestQueueRecovery.startEvents) {
        await e.wait();
      }
      expect(TestQueueRecovery.cnt).toBe(4);
      expect((await wfh1.getStatus())?.status).toBe(StatusString.PENDING);
      expect((await wfh2.getStatus())?.status).toBe(StatusString.PENDING);
      expect((await wfh3.getStatus())?.status).toBe(StatusString.ENQUEUED);

      // Unblock the two first workflows
      TestQueueRecovery.stopEvent.set();
      // Verify all queue entries eventually get cleaned up.
      expect(await wfh1.getResult()).toBe(null);
      expect(await wfh2.getResult()).toBe(null);
      expect(await wfh3.getResult()).toBe(null);
      const result = await systemDBClient.query(
        'SELECT executor_id FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfh3.workflowID],
      );
      expect(result.rows).toEqual([{ executor_id: 'local' }]);
      expect(await queueEntriesAreCleanedUp()).toBe(true);
    } finally {
      await systemDBClient.end();
    }
  }, 20000);

  class TestCancelQueues {
    static startEvent = new Event();
    static blockingEvent = new Event();
    static queue = new WorkflowQueue('TestCancelQueues', { concurrency: 1 });

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
    await DBOSExecutor.globalInstance?.cancelWorkflow(wfid);
    await expect(blockedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
    await expect(regularHandle.getResult()).resolves.toBeNull();

    // Complete the blocked workflow
    TestCancelQueues.blockingEvent.set();
    await expect(blockedHandle.getResult()).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);

    // Verify all queue entries eventually get cleaned up
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });

  class TestResumeQueues {
    static startEvent = new Event();
    static blockingEvent = new Event();
    static queue = new WorkflowQueue('TestResumeQueues', { concurrency: 1 });

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

    await DBOSExecutor.globalInstance?.resumeWorkflow(wfid);

    await expect(regularHandle.getResult()).resolves.toBeNull();

    // Complete the blocked workflow. Verify the second regular workflow also completes.
    TestResumeQueues.blockingEvent.set();
    await expect(blockedHandle.getResult()).resolves.toBeNull();
    await expect(regularHandleTwo.getResult()).resolves.toBeNull();

    // Verify all queue entries eventually get cleaned up
    expect(await queueEntriesAreCleanedUp()).toBe(true);
  });

  class TestConcurrencyAcrossVersions {
    static queue = new WorkflowQueue('TestAcrossVersions', { workerConcurrency: 1 });

    @DBOS.workflow()
    static async testWorkflow() {
      return Promise.resolve(DBOS.workflowID);
    }
  }

  test('test-concurrency-across-versions', async () => {
    const connectionString = DBOS.dbosConfig?.poolConfig?.connectionString;
    if (!connectionString) {
      throw new Error('DBOS is not configured with a connection string');
    }
    const client = await DBOSClient.create(connectionString);

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
});

// dummy declaration to match the workflow in tests/wfqueueworker.ts
export class InterProcessWorkflowTask {
  @DBOS.workflow()
  static async task(_: number) {
    return Promise.resolve();
  }
}

// This queue cannot dequeue
const IPWQueue = new WorkflowQueue('IPWQueue', {
  rateLimit: { limitPerPeriod: 0, periodSec: 30 },
});
class InterProcessWorkflow {
  static localConcurrencyLimit = 5;
  static globalConcurrencyLimit = InterProcessWorkflow.localConcurrencyLimit * 2;

  @DBOS.workflow()
  static async testGlobalConcurrency(config: DBOSConfigInternal) {
    // First, start local concurrency limit tasks
    let handles = [];
    for (let i = 0; i < InterProcessWorkflow.localConcurrencyLimit; ++i) {
      handles.push(await DBOS.startWorkflow(InterProcessWorkflowTask, { queueName: IPWQueue.name }).task(i));
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
        handles.push(await DBOS.startWorkflow(InterProcessWorkflowTask, { queueName: IPWQueue.name }).task(i));
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
          await DBOS.startWorkflow(InterProcessWorkflowTask, { queueName: IPWQueue.name }).task(
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
        user: config.poolConfig.user,
        port: config.poolConfig.port,
        host: config.poolConfig.host,
        password: config.poolConfig.password,
        database: config.system_database,
      });
      await systemDBClient.connect();
      try {
        const result = await systemDBClient.query<{ count: string }>(
          'SELECT COUNT(*) FROM dbos.workflow_status WHERE status = $1 AND queue_name = $2',
          [StatusString.PENDING, IPWQueue.name],
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

  @DBOS.step()
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
            `${IPWQueue.name}`,
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
  let config: DBOSConfigInternal;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('test_global_and_local_concurrency', async () => {
    const wfh = await DBOS.startWorkflow(InterProcessWorkflow).testGlobalConcurrency(config);
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

async function runOneAtATime(queue: WorkflowQueue) {
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
  let config: DBOSConfigInternal;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  class TestExample {
    static resolveEvent: () => void;
    static workflowEvent = new Promise<void>((resolve) => {
      TestExample.resolveEvent = resolve;
    });

    static queue = new WorkflowQueue('test_dedup_queue', { concurrency: 1 });
    static queue2 = new WorkflowQueue('queue2', { concurrency: 1 });

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
  }, 20000);

  class TestPriority {
    static resolveEvent: () => void;
    static workflowEvent = new Promise<void>((resolve) => {
      TestPriority.resolveEvent = resolve;
    });

    static wfPriorityList: number[] = [];

    static queue = new WorkflowQueue('test_queue_prority', { concurrency: 1 });
    static childqueue = new WorkflowQueue('child_queue', { concurrency: 1 });

    @DBOS.workflow()
    static async parentWorkflow(input: number): Promise<number> {
      // 0 means no priority
      TestPriority.wfPriorityList.push(input);

      const wfh1 = await DBOS.startWorkflow(TestPriority, {
        queueName: childqueue.name,
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
  }, 30000);
});

describe('queue-time-outs', () => {
  let config: DBOSConfigInternal;

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
      await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: timeout, queueName: queue.name })
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
      await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: null, queueName: queue.name })
        .sleepingWorkflow(duration * 2)
        .then((h) => h.getResult());
    }
  }

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  // enqueue workflow with timeout
  test('enqueue-workflow-withWorkflowTimeout', async () => {
    const workflowID: string = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: queue.name,
      timeoutMS: 100,
    }).blockedWorkflow();
    await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(workflowID));
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.CANCELLED);
  });

  // enqueue workflow with *no* timeout which calls a blocked child with timeout
  test('enqueue-workflow-withChildTimeout', async () => {
    const workflowID: string = randomUUID();
    const childID: string = `${workflowID}-0`;
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      queueName: queue.name,
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
      queueName: queue.name,
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
      queueName: queue.name,
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
      queueName: queue.name,
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
      queueName: queue.name,
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
      queueName: queue.name,
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
      queueName: queue.name,
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
});
