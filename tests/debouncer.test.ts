import { randomUUID } from 'node:crypto';
import { DBOS, Debouncer, DBOSConfig, StatusString, DBOSClient, DebouncerClient } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { DBOSQueueDuplicatedError } from '../src/error';
import { INTERNAL_QUEUE_NAME } from '../src/utils';
import { clearDebugTriggers, DebugAction, DEBUG_TRIGGER_STEP_COMMIT, setDebugTrigger } from '../src/debugpoint';
import { PortableWorkflowError } from '../schemas/system_db_schema';
import { generateDBOSTestConfig, reexecuteWorkflowById, setUpDBOSTestSysDb, Event } from './helpers';
import assert from 'node:assert';

describe('debouncer-tests', () => {
  let config: DBOSConfig;

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
    await DBOS.registerQueue(queue.name, { onConflict: 'always_update' });
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  // Register a simple workflow for testing
  const workflow = DBOS.registerWorkflow(
    async (x: number) => {
      return Promise.resolve(x);
    },
    { name: 'debouncerWorkflow' },
  );

  const queue = { name: 'test-queue' };

  // The dedup ID a debounce computes for `workflow` and a given key.
  const dedupIDForKey = (key: string) => `.debouncerWorkflow-${key}`;
  const getSysDB = () => DBOSExecutor.globalInstance!.systemDatabase;

  // A workflow that signals when it starts and blocks until released, reset per test.
  let blockStarted: Event;
  let blockRelease: Event;
  const blockingWorkflow = DBOS.registerWorkflow(
    async (x: number) => {
      blockStarted.set();
      await blockRelease.wait();
      return x;
    },
    { name: 'debouncerBlockingWorkflow' },
  );

  let deadlineChildRan = false;
  const deadlineChild = DBOS.registerWorkflow(
    async (x: number) => {
      deadlineChildRan = true;
      return Promise.resolve(x);
    },
    { name: 'debouncerDeadlineChild' },
  );
  const deadlineDebouncer = new Debouncer({ workflow: deadlineChild });
  // Debounces with a delay longer than the caller's own timeout, then returns the debounced workflow's ID.
  const deadlineParent = DBOS.registerWorkflow(
    async () => {
      const handle = await deadlineDebouncer.debounce('deadline-key', 5000, 5);
      return handle.workflowID;
    },
    { name: 'debouncerDeadlineParent' },
  );

  let replayChildRuns = 0;
  let replayParentRuns = 0;
  const replayChild = DBOS.registerWorkflow(
    async (x: number) => {
      replayChildRuns++;
      return Promise.resolve(x);
    },
    { name: 'debouncerReplayChild' },
  );
  const replayDebouncer = new Debouncer({ workflow: replayChild });
  const replayParent = DBOS.registerWorkflow(
    async () => {
      replayParentRuns++;
      const handle = await replayDebouncer.debounce('replay-key', 1000, 7);
      return handle.workflowID;
    },
    { name: 'debouncerReplayParent' },
  );

  const leakOther = DBOS.registerWorkflow(
    async (x: number) => {
      return Promise.resolve(x);
    },
    { name: 'debouncerLeakOther' },
  );
  const leakDebouncer = new Debouncer({ workflow });
  // The first call creates the delayed workflow; the pinned second call bounces it, so the pinned ID goes unused.
  const leakParent = DBOS.registerWorkflow(
    async (pinnedID: string) => {
      const first = await leakDebouncer.debounce('leak-key', 1000000000, 1);
      const bounced = await DBOS.withNextWorkflowID(pinnedID, () => leakDebouncer.debounce('leak-key', 1000000000, 2));
      assert.equal(bounced.workflowID, first.workflowID);
      const next = await DBOS.startWorkflow(leakOther)(3);
      return [first.workflowID, next.workflowID];
    },
    { name: 'debouncerLeakParent' },
  );

  let atomicRuns = 0;
  const atomicWorkflow = DBOS.registerWorkflow(
    async (x: number) => {
      atomicRuns++;
      return Promise.resolve(x);
    },
    { name: 'debouncerAtomicWorkflow' },
  );
  const atomicDebouncer = new Debouncer({ workflow: atomicWorkflow });
  const atomicParent = DBOS.registerWorkflow(
    async () => {
      const handle = await atomicDebouncer.debounce('atomic-key', 2000000000, 2);
      return handle.workflowID;
    },
    { name: 'debouncerAtomicParent' },
  );

  const postCommitWorkflow = DBOS.registerWorkflow(
    async (x: number) => {
      return Promise.resolve(x);
    },
    { name: 'debouncerPostCommitWorkflow' },
  );
  const postCommitDebouncer = new Debouncer({ workflow: postCommitWorkflow });
  const postCommitParent = DBOS.registerWorkflow(
    async () => {
      const handle = await postCommitDebouncer.debounce('post-commit-key', 2000000000, 2);
      return handle.workflowID;
    },
    { name: 'debouncerPostCommitParent' },
  );

  test('test-debouncer-workflow', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const debouncePeriodMs = 2000;

    const test = async () => {
      const debouncer1 = new Debouncer({
        workflow,
      });
      const firstHandle = await debouncer1.debounce('key', debouncePeriodMs, firstValue);

      const debouncer2 = new Debouncer({
        workflow,
      });
      const secondHandle = await debouncer2.debounce('key', debouncePeriodMs, secondValue);

      assert.equal(firstHandle.workflowID, secondHandle.workflowID);
      assert.equal(await firstHandle.getResult(), secondValue);
      assert.equal(await secondHandle.getResult(), secondValue);

      const debouncer3 = new Debouncer({
        workflow,
      });
      const thirdHandle = await debouncer3.debounce('key', debouncePeriodMs, thirdValue);

      const debouncer4 = new Debouncer({
        workflow,
      });
      const fourthHandle = await debouncer4.debounce('key', debouncePeriodMs, fourthValue);

      assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
      assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
      assert.equal(await thirdHandle.getResult(), fourthValue);
      assert.equal(await fourthHandle.getResult(), fourthValue);
    };
    // First, run the test operations directly
    await test();
    // Then, run the test operations inside a workflow and verify they work there
    await DBOS.shutdown();
    const testWorkflow = DBOS.registerWorkflow(test);
    await DBOS.launch();
    await DBOS.registerQueue(queue.name, { onConflict: 'always_update' });
    const originalHandle = await DBOS.startWorkflow(testWorkflow)();
    await originalHandle.getResult();

    // Rerun the workflow, verify it still works
    const recoverHandle = await reexecuteWorkflowById(originalHandle.workflowID);
    await recoverHandle!.getResult();
    const status = await recoverHandle!.getStatus();
    assert.equal(status?.status, StatusString.SUCCESS);
  });

  test('test-debouncer-timeout', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const longDebouncePeriodMs = 10000000000;
    const shortDebouncePeriodMs = 1000;

    // Set a huge period but small timeout, verify workflows start after the timeout
    const debouncer1 = new Debouncer({
      workflow,
      debounceTimeoutMs: 2000,
    });

    const firstHandle = await debouncer1.debounce('key', longDebouncePeriodMs, firstValue);
    const secondHandle = await debouncer1.debounce('key', longDebouncePeriodMs, secondValue);

    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);

    const thirdHandle = await debouncer1.debounce('key', longDebouncePeriodMs, thirdValue);
    const fourthHandle = await debouncer1.debounce('key', longDebouncePeriodMs, fourthValue);

    assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);

    const debouncer2 = new Debouncer({
      workflow,
    });

    const fifthHandle = await debouncer2.debounce('key', longDebouncePeriodMs, firstValue);
    const sixthHandle = await debouncer2.debounce('key', shortDebouncePeriodMs, secondValue);

    assert.notEqual(fourthHandle.workflowID, fifthHandle.workflowID);
    assert.equal(fifthHandle.workflowID, sixthHandle.workflowID);
    assert.equal(await fifthHandle.getResult(), secondValue);
    assert.equal(await sixthHandle.getResult(), secondValue);
  });

  test('test-multiple-debouncers', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const debouncePeriodMs = 2000;

    // Create two debouncers and use two different keys
    const debouncerOne = new Debouncer({
      workflow,
    });

    const debouncerTwo = new Debouncer({
      workflow,
    });

    const firstHandle = await debouncerOne.debounce('key_one', debouncePeriodMs, firstValue);
    const secondHandle = await debouncerOne.debounce('key_one', debouncePeriodMs, secondValue);
    const thirdHandle = await debouncerTwo.debounce('key_two', debouncePeriodMs, thirdValue);
    const fourthHandle = await debouncerTwo.debounce('key_two', debouncePeriodMs, fourthValue);

    // Verify that debouncers with different keys create different workflows
    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.notEqual(firstHandle.workflowID, thirdHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);

    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);
  });

  test('test-debouncer-queue', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const debouncePeriodMs = 2000;

    const debouncer = new Debouncer({
      workflow,
      startWorkflowParams: { queueName: queue.name, timeoutMS: 5000 },
    });

    const firstHandle = await debouncer.debounce('key', debouncePeriodMs, firstValue);
    const secondHandle = await debouncer.debounce('key', debouncePeriodMs, secondValue);

    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);

    const secondStatus = await secondHandle.getStatus();
    assert.equal(secondStatus?.queueName, queue.name);

    const thirdHandle = await debouncer.debounce('key', debouncePeriodMs, thirdValue);
    const fourthHandle = await debouncer.debounce('key', debouncePeriodMs, fourthValue);

    assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);

    const fourthStatus = await fourthHandle.getStatus();
    assert.equal(fourthStatus?.queueName, queue.name);
    assert.equal(fourthStatus?.timeoutMS, 5000);
    assert(fourthStatus?.deadlineEpochMS);
  });

  test('test-debouncer-client', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const longDebouncePeriodMs = 10000000000;
    const shortDebouncePeriodMs = 1000;

    // Set a huge period but small timeout, verify workflows start after the timeout
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    const debouncer1 = new DebouncerClient(client, {
      workflowName: 'debouncerWorkflow',
      debounceTimeoutMs: 2000,
    });

    const firstHandle = await debouncer1.debounce('key', longDebouncePeriodMs, firstValue);
    const secondHandle = await debouncer1.debounce('key', longDebouncePeriodMs, secondValue);

    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);

    const thirdHandle = await debouncer1.debounce('key', longDebouncePeriodMs, thirdValue);
    const fourthHandle = await debouncer1.debounce('key', longDebouncePeriodMs, fourthValue);

    assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);

    const debouncer2 = new DebouncerClient(client, {
      workflowName: 'debouncerWorkflow',
    });

    const fifthHandle = await debouncer2.debounce('key', longDebouncePeriodMs, firstValue);
    const sixthHandle = await debouncer2.debounce('key', shortDebouncePeriodMs, secondValue);

    assert.notEqual(fourthHandle.workflowID, fifthHandle.workflowID);
    assert.equal(fifthHandle.workflowID, sixthHandle.workflowID);
    assert.equal(await fifthHandle.getResult(), secondValue);
    assert.equal(await sixthHandle.getResult(), secondValue);
    await client.destroy();
  }, 60000);

  class TestClass {
    @DBOS.workflow()
    static async exampleWorkflow(x: number) {
      return Promise.resolve(x);
    }
  }

  test('test-debouncer-class', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const fifthValue = 4;
    const sixthValue = 5;
    const debouncePeriodMs = 2000;

    // Test that two calls with the same key merge into one workflow
    const debouncer = new Debouncer({
      workflow: TestClass.exampleWorkflow,
    });
    const firstHandle = await debouncer.debounce('key', debouncePeriodMs, firstValue);

    const secondHandle = await debouncer.debounce('key', debouncePeriodMs, secondValue);

    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);

    const thirdHandle = await debouncer.debounce('key', debouncePeriodMs, thirdValue);
    const fourthHandle = await debouncer.debounce('key', debouncePeriodMs, fourthValue);

    assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);

    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    const clientDebouncer = new DebouncerClient(client, {
      workflowClassName: 'TestClass',
      workflowName: 'exampleWorkflow',
      debounceTimeoutMs: 2000,
    });

    const fifthHandle = await clientDebouncer.debounce('key', debouncePeriodMs, fifthValue);
    const sixthHandle = await clientDebouncer.debounce('key', debouncePeriodMs, sixthValue);

    assert.notEqual(fourthHandle.workflowID, fifthHandle.workflowID);
    assert.equal(fifthHandle.workflowID, sixthHandle.workflowID);
    assert.equal(await fifthHandle.getResult(), sixthValue);
    assert.equal(await fifthHandle.getResult(), sixthValue);
    await client.destroy();
  });

  test('test-debounce-flip-clears-dedup', async () => {
    blockStarted = new Event();
    blockRelease = new Event();

    const debouncer = new Debouncer({ workflow: blockingWorkflow });
    // A huge period keeps the workflow DELAYED until a later bounce shortens it.
    const handle = await debouncer.debounce('key', 1000000000, 1);

    // While delayed, the workflow is marked debounced and holds the debounce key.
    let status = await getSysDB().getWorkflowStatus(handle.workflowID);
    expect(status?.status).toBe(StatusString.DELAYED);
    expect(status?.isDebounced).toBe(true);
    expect(status?.deduplicationID).toBe('.debouncerBlockingWorkflow-key');

    // A bounce shortens the delay and replaces the input; the same workflow runs.
    const handle2 = await debouncer.debounce('key', 1000, 2);
    expect(handle2.workflowID).toBe(handle.workflowID);

    // Wait for the flip: the workflow starts but blocks, so it's in flight, not complete.
    await blockStarted.wait();

    // The flip cleared the debounce key; assert while still running since completion also clears it.
    status = await getSysDB().getWorkflowStatus(handle.workflowID);
    expect(status?.status).toBe(StatusString.PENDING);
    expect(status?.deduplicationID).toBeUndefined();

    // A same-key debounce therefore starts fresh even before the first workflow finishes.
    const handle3 = await debouncer.debounce('key', 1000, 3);
    expect(handle3.workflowID).not.toBe(handle.workflowID);

    blockRelease.set();
    expect(await handle2.getResult()).toBe(2);
    expect(await handle3.getResult()).toBe(3);
  }, 60000);

  test('test-debounce-cancel-then-redebounce', async () => {
    const debouncer = new Debouncer({ workflow });
    const handle = await debouncer.debounce('key', 1000000000, 1);
    const status = await getSysDB().getWorkflowStatus(handle.workflowID);
    expect(status?.status).toBe(StatusString.DELAYED);

    // Cancelling a delayed debounced workflow clears its debounce key.
    await DBOS.cancelWorkflow(handle.workflowID);

    // A new debounce with the same key therefore starts a fresh workflow.
    const handle2 = await debouncer.debounce('key', 1000, 2);
    expect(handle2.workflowID).not.toBe(handle.workflowID);
    expect(await handle2.getResult()).toBe(2);
  }, 30000);

  test('test-debounce-deadline-caps-initial-delay', async () => {
    // A huge period with a finite timeout: the delay is capped at the deadline.
    const debouncer = new Debouncer({ workflow, debounceTimeoutMs: 1000000 });
    const handle = await debouncer.debounce('key', 1000000000, 1);

    const status = await getSysDB().getWorkflowStatus(handle.workflowID);
    expect(status?.status).toBe(StatusString.DELAYED);
    expect(status?.isDebounced).toBe(true);
    expect(status?.debounceDeadlineEpochMS).toBeDefined();
    expect(status?.delayUntilEpochMS).toBeDefined();
    expect(status!.delayUntilEpochMS!).toBeLessThanOrEqual(status!.debounceDeadlineEpochMS!);

    await DBOS.cancelWorkflow(handle.workflowID);
  });

  test('test-debounce-user-dedup-conflict-raises', async () => {
    // A non-debounced workflow occupies the debounce key on the internal queue.
    const dedupID = dedupIDForKey('key');
    const blocker = await DBOS.startWorkflow(workflow, {
      queueName: INTERNAL_QUEUE_NAME,
      enqueueOptions: { deduplicationID: dedupID, delaySeconds: 100000 },
    })(1);

    // The key is held by a non-debounced workflow, so debouncing it surfaces the dedup conflict rather than hijacking it.
    const debouncer = new Debouncer({ workflow });
    await expect(debouncer.debounce('key', 1000, 2)).rejects.toThrow(DBOSQueueDuplicatedError);

    await DBOS.cancelWorkflow(blocker.workflowID);
  });

  test('test-debounce-fixed-workflow-id-reuse', async () => {
    const debouncer = new Debouncer({ workflow });

    // Pinning the same workflow ID across debounces of one key must still bounce the existing workflow (last input wins), not drop it on a workflow_uuid collision.
    const wfid = randomUUID();
    const firstHandle = await DBOS.withNextWorkflowID(wfid, () => debouncer.debounce('key', 2000, 1));
    const secondHandle = await DBOS.withNextWorkflowID(wfid, () => debouncer.debounce('key', 2000, 2));
    expect(firstHandle.workflowID).toBe(wfid);
    expect(secondHandle.workflowID).toBe(wfid);
    expect(await firstHandle.getResult()).toBe(2);
    expect(await secondHandle.getResult()).toBe(2);

    // A huge initial period then a short one under the same pinned ID must still shorten the delay so the workflow runs.
    const wfid2 = randomUUID();
    const thirdHandle = await DBOS.withNextWorkflowID(wfid2, () => debouncer.debounce('key2', 1000000000, 3));
    const fourthHandle = await DBOS.withNextWorkflowID(wfid2, () => debouncer.debounce('key2', 1000, 4));
    expect(thirdHandle.workflowID).toBe(wfid2);
    expect(fourthHandle.workflowID).toBe(wfid2);
    expect(await fourthHandle.getResult()).toBe(4);
  }, 30000);

  test('test-debounce-inworkflow-does-not-inherit-parent-deadline', async () => {
    deadlineChildRan = false;

    // Debounce inside a workflow whose timeout (2s) is shorter than the debounce delay (5s); the debounced workflow must not inherit the parent's deadline or it would be cancelled before running.
    const parentHandle = await DBOS.startWorkflow(deadlineParent, { timeoutMS: 2000 })();
    const childID = await parentHandle.getResult();

    // The debounced workflow carries no inherited deadline while delayed.
    const status = await getSysDB().getWorkflowStatus(childID);
    expect(status?.status).toBe(StatusString.DELAYED);
    expect(status?.deadlineEpochMS).toBeUndefined();

    // It runs to completion after the delay rather than being cancelled.
    const childHandle = DBOS.retrieveWorkflow<number>(childID);
    expect(await childHandle.getResult()).toBe(5);
    expect(deadlineChildRan).toBe(true);
    expect((await childHandle.getStatus())?.status).toBe(StatusString.SUCCESS);
  }, 60000);

  test('test-debounce-inworkflow-replay-is-deterministic', async () => {
    replayChildRuns = 0;
    replayParentRuns = 0;

    const parentHandle = await DBOS.startWorkflow(replayParent)();
    const childID = await parentHandle.getResult();
    expect(replayParentRuns).toBe(1);

    // Force the parent to replay from its checkpoints: the bounce step and enqueue are checkpointed, so replay must re-run the body, return the same child ID, and not enqueue a second child.
    const recoverHandle = await reexecuteWorkflowById(parentHandle.workflowID);
    expect(await recoverHandle!.getResult()).toBe(childID);
    expect(replayParentRuns).toBe(2);

    // Exactly one child workflow exists for this key: replay did not double-enqueue.
    const children = await DBOS.listWorkflows({ workflowName: 'debouncerReplayChild' });
    expect(children.length).toBe(1);

    // The single debounced child runs to completion exactly once.
    const childHandle = DBOS.retrieveWorkflow<number>(childID);
    expect(await childHandle.getResult()).toBe(7);
    expect(replayChildRuns).toBe(1);
  }, 60000);

  test('test-debounce-retries-replayed-portable-dedup-error', async () => {
    // Regression test: when a debounce's fresh enqueue loses the dedup race in a workflow using
    // portable serialization, replay revives the checkpointed error as a PortableWorkflowError
    // carrying only the original type name. The retry loop must still recognize that form and
    // bounce the existing workflow instead of erroring.
    const debouncer = new Debouncer({ workflow });
    const sysDB = getSysDB();
    const originalInit = sysDB.initWorkflowStatus.bind(sysDB);
    let dedupInitCalls = 0;
    const spy = jest.spyOn(sysDB, 'initWorkflowStatus').mockImplementation(async (initStatus, ownerXid, options) => {
      // The first enqueue throws the exact object a replay produces from a checkpointed, portable-serialized dedup error; subsequent enqueues behave normally.
      if (initStatus.deduplicationID === dedupIDForKey('portable-key')) {
        dedupInitCalls++;
        if (dedupInitCalls === 1) {
          throw new PortableWorkflowError('Workflow was deduplicated', DBOSQueueDuplicatedError.name);
        }
      }
      return originalInit(initStatus, ownerXid, options);
    });

    try {
      const handle = await debouncer.debounce('portable-key', 500, 5);
      // The loop recognized the replay-form dedup error and retried instead of propagating it.
      expect(dedupInitCalls).toBe(2);
      expect(await handle.getResult()).toBe(5);
    } finally {
      spy.mockRestore();
    }
  }, 30000);

  test('test-debounce-rejects-caller-dedup-and-delay', async () => {
    // A debounce owns the workflow's deduplication ID and delay, and priority/partition keys cannot apply to a debounced enqueue, so a caller that sets any of them must fail loudly rather than have it silently ignored or break later.

    // Local: a caller-set deduplicationID is rejected.
    let debouncer = new Debouncer({
      workflow,
      startWorkflowParams: { enqueueOptions: { deduplicationID: 'caller-dedup' } },
    });
    await expect(debouncer.debounce('k', 1000, 1)).rejects.toThrow('deduplicationID');

    // Local: a caller-set delay is rejected.
    debouncer = new Debouncer({ workflow, startWorkflowParams: { enqueueOptions: { delaySeconds: 100 } } });
    await expect(debouncer.debounce('k', 1000, 1)).rejects.toThrow('delay');

    // Local: a caller-set priority is rejected.
    debouncer = new Debouncer({ workflow, startWorkflowParams: { enqueueOptions: { priority: 1 } } });
    await expect(debouncer.debounce('k', 1000, 1)).rejects.toThrow('priority');

    // Local: a caller-set partition key is rejected.
    debouncer = new Debouncer({
      workflow,
      startWorkflowParams: { enqueueOptions: { queuePartitionKey: 'caller-partition' } },
    });
    await expect(debouncer.debounce('k', 1000, 1)).rejects.toThrow('partition key');

    // No conflicting option left a workflow behind.
    expect((await DBOS.listWorkflows({ workflowName: 'debouncerWorkflow' })).length).toBe(0);

    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      // Client: the same four options are rejected.
      const dedupClient = new DebouncerClient(client, {
        workflowName: 'debouncerWorkflow',
        startWorkflowParams: { enqueueOptions: { deduplicationID: 'caller-dedup' } },
      });
      await expect(dedupClient.debounce('k', 1000, 1)).rejects.toThrow('deduplicationID');

      const delayClient = new DebouncerClient(client, {
        workflowName: 'debouncerWorkflow',
        startWorkflowParams: { enqueueOptions: { delaySeconds: 100 } },
      });
      await expect(delayClient.debounce('k', 1000, 1)).rejects.toThrow('delay');

      const priorityClient = new DebouncerClient(client, {
        workflowName: 'debouncerWorkflow',
        startWorkflowParams: { enqueueOptions: { priority: 1 } },
      });
      await expect(priorityClient.debounce('k', 1000, 1)).rejects.toThrow('priority');

      const partitionClient = new DebouncerClient(client, {
        workflowName: 'debouncerWorkflow',
        startWorkflowParams: { enqueueOptions: { queuePartitionKey: 'caller-partition' } },
      });
      await expect(partitionClient.debounce('k', 1000, 1)).rejects.toThrow('partition key');
    } finally {
      await client.destroy();
    }
  });

  test('test-debounce-bounce-path-does-not-leak-pinned-id', async () => {
    // Regression test: a pinned workflow ID around a debounce that coalesces (bounce path) is captured by the debounce and must not stay armed on the workflow's context, or the next child workflow the parent starts would silently run under the pinned ID.
    const pinnedID = randomUUID();
    const [delayedID, nextID] = await (await DBOS.startWorkflow(leakParent)(pinnedID)).getResult();

    // The next child workflow did not inherit the unused pinned ID.
    expect(nextID).not.toBe(pinnedID);
    expect(await DBOS.retrieveWorkflow<number>(nextID).getResult()).toBe(3);

    await DBOS.cancelWorkflow(delayedID);
  }, 30000);

  test('test-debounce-pinned-id-survives-enqueue-dedup-race', async () => {
    // Regression test: a fresh-enqueue attempt consumes a pinned workflow ID from the context before its INSERT can lose the dedup race. The retry loop must re-apply the pinned ID so the workflow is still created under it, not a generated UUID.
    const debouncer = new Debouncer({ workflow });
    const sysDB = getSysDB();
    const originalInit = sysDB.initWorkflowStatus.bind(sysDB);
    let initCalls = 0;
    const spy = jest.spyOn(sysDB, 'initWorkflowStatus').mockImplementation(async (initStatus, ownerXid, options) => {
      // The first enqueue's insert loses the dedup race after the pinned ID was already consumed; later calls behave normally.
      if (initStatus.deduplicationID === dedupIDForKey('pin-race-key')) {
        initCalls++;
        if (initCalls === 1) {
          throw new DBOSQueueDuplicatedError('racer', 'queue', 'dedup');
        }
      }
      return originalInit(initStatus, ownerXid, options);
    });

    try {
      const wfid = randomUUID();
      const handle = await DBOS.withNextWorkflowID(wfid, () => debouncer.debounce('pin-race-key', 500, 7));

      // The retried enqueue reused the pinned ID rather than minting a random one.
      expect(initCalls).toBeGreaterThanOrEqual(2);
      expect(handle.workflowID).toBe(wfid);
      expect(await handle.getResult()).toBe(7);
    } finally {
      spy.mockRestore();
    }
  }, 30000);

  test('test-debounce-key-collision-between-workflows', async () => {
    // Regression test: "colw"+"b-k" and "colw-b"+"k" both yield dedup ID ".colw-b-k".
    // The collider must surface the conflict, not overwrite the other workflow's row.
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const debouncerA = new DebouncerClient(client, { workflowName: 'colw' });
      const debouncerB = new DebouncerClient(client, { workflowName: 'colw-b' });

      // A huge period keeps workflow "colw" DELAYED, holding dedup ID ".colw-b-k".
      const handleA = await debouncerA.debounce('b-k', 1000000000, 1);
      let status = await getSysDB().getWorkflowStatus(handleA.workflowID);
      expect(status?.status).toBe(StatusString.DELAYED);
      expect(status?.deduplicationID).toBe('.colw-b-k');

      // The colliding debounce for workflow "colw-b" raises instead of hijacking the row.
      await expect(debouncerB.debounce('k', 1000, 2)).rejects.toThrow(DBOSQueueDuplicatedError);

      // The victim is untouched: still DELAYED under its own name.
      status = await getSysDB().getWorkflowStatus(handleA.workflowID);
      expect(status?.status).toBe(StatusString.DELAYED);
      expect(status?.workflowName).toBe('colw');

      // A same-name bounce still coalesces onto the existing workflow.
      const handleA2 = await debouncerA.debounce('b-k', 1000000000, 3);
      expect(handleA2.workflowID).toBe(handleA.workflowID);

      await DBOS.cancelWorkflow(handleA.workflowID);
    } finally {
      await client.destroy();
    }
  });

  test('test-debounce-bounce-atomic-with-checkpoint', async () => {
    // Regression test (exactly-once): an in-workflow bounce commits atomically with its step
    // checkpoint. If the checkpoint write fails, the bounce must roll back with it; otherwise a
    // recovered parent would re-bounce or re-enqueue work that already committed, running it twice.
    atomicRuns = 0;

    // A fresh debounced workflow, kept DELAYED by a huge period.
    const wHandle = await atomicDebouncer.debounce('atomic-key', 1000000000, 1);
    let status = await getSysDB().getWorkflowStatus(wHandle.workflowID);
    expect(status?.status).toBe(StatusString.DELAYED);
    const delayBefore = status!.delayUntilEpochMS!;
    expect(delayBefore).toBeDefined();

    // The bounce's checkpoint write fails once, after the bounce UPDATE ran in the same transaction.
    type RecordOpFn = (...args: unknown[]) => Promise<unknown>;
    const sysDBInternals = getSysDB() as unknown as { recordOperationResultInternal: RecordOpFn };
    const originalRecord = sysDBInternals.recordOperationResultInternal.bind(getSysDB());
    let armed = true;
    const spy = jest.spyOn(sysDBInternals, 'recordOperationResultInternal').mockImplementation((...args: unknown[]) => {
      if (armed && args[3] === 'DBOS.debounceDelayedWorkflow') {
        armed = false;
        throw new Error('crash before checkpoint commit');
      }
      return originalRecord(...args);
    });

    let parentID: string;
    try {
      const parentHandle = await DBOS.startWorkflow(atomicParent)();
      parentID = parentHandle.workflowID;
      await expect(parentHandle.getResult()).rejects.toThrow('crash before checkpoint commit');
    } finally {
      spy.mockRestore();
    }

    // The failed checkpoint rolled back the bounce with it: the delay (set in the same UPDATE as the inputs) is unchanged.
    status = await getSysDB().getWorkflowStatus(wHandle.workflowID);
    expect(status?.status).toBe(StatusString.DELAYED);
    expect(status?.delayUntilEpochMS).toBe(delayBefore);

    // Recovery replays the parent: no checkpoint exists, so the bounce re-executes and lands exactly once.
    const recoverHandle = await reexecuteWorkflowById(parentID);
    expect(await recoverHandle!.getResult()).toBe(wHandle.workflowID);

    status = await getSysDB().getWorkflowStatus(wHandle.workflowID);
    expect(status!.delayUntilEpochMS!).toBeGreaterThan(delayBefore);

    // Exactly one workflow exists for the key; shortening the delay runs it exactly once with the bounced input.
    expect((await DBOS.listWorkflows({ workflowName: 'debouncerAtomicWorkflow' })).length).toBe(1);
    const finalHandle = await atomicDebouncer.debounce('atomic-key', 500, 2);
    expect(finalHandle.workflowID).toBe(wHandle.workflowID);
    expect(await finalHandle.getResult()).toBe(2);
    expect(atomicRuns).toBe(1);
  }, 60000);

  test('test-debounce-bounce-checkpoint-survives-post-commit-crash', async () => {
    // Regression test (exactly-once): a crash immediately after the atomic bounce+checkpoint
    // commit must replay through the checkpoint on recovery -- same handle, no second bounce.
    const wHandle = await postCommitDebouncer.debounce('post-commit-key', 1000000000, 1);
    let status = await getSysDB().getWorkflowStatus(wHandle.workflowID);
    expect(status?.status).toBe(StatusString.DELAYED);
    const delayBefore = status!.delayUntilEpochMS!;
    expect(delayBefore).toBeDefined();

    // Crash the parent right after the bounce's transaction commits.
    const action = new DebugAction();
    action.callback = () => {
      throw new Error('crash after checkpoint commit');
    };
    setDebugTrigger(DEBUG_TRIGGER_STEP_COMMIT, action);
    let parentID: string;
    try {
      const parentHandle = await DBOS.startWorkflow(postCommitParent)();
      parentID = parentHandle.workflowID;
      await expect(parentHandle.getResult()).rejects.toThrow('crash after checkpoint commit');
    } finally {
      clearDebugTriggers();
    }

    // The bounce and its checkpoint both committed before the crash.
    status = await getSysDB().getWorkflowStatus(wHandle.workflowID);
    expect(status!.delayUntilEpochMS!).toBeGreaterThan(delayBefore);
    const delayAfterCrash = status!.delayUntilEpochMS!;

    // Replay short-circuits through the checkpoint: same handle, and no re-executed bounce re-extends the delay.
    const recoverHandle = await reexecuteWorkflowById(parentID);
    expect(await recoverHandle!.getResult()).toBe(wHandle.workflowID);

    status = await getSysDB().getWorkflowStatus(wHandle.workflowID);
    expect(status?.delayUntilEpochMS).toBe(delayAfterCrash);

    await DBOS.cancelWorkflow(wHandle.workflowID);
  }, 60000);
});
