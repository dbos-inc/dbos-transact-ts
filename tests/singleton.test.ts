import { DBOS } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

const testPolling = { minPollingIntervalMs: 100 };

describe('singleton workflows', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    SingletonTest.resetEvent();
    await DBOS.launch();
    await DBOS.registerQueue(SingletonTest.queue.name, {
      onConflict: 'always_update',
      ...SingletonTest.queue.config,
    });
  });

  afterEach(async () => {
    SingletonTest.resolveEvent();
    await DBOS.shutdown();
  });

  class SingletonTest {
    static queue = {
      name: 'singleton_queue',
      config: { concurrency: 1, ...testPolling },
    };

    static resolveEvent: () => void = () => {};
    static workflowEvent: Promise<void> = Promise.resolve();
    static markerStepRuns = 0;
    static resetEvent() {
      SingletonTest.workflowEvent = new Promise<void>((resolve) => {
        SingletonTest.resolveEvent = resolve;
      });
    }

    @DBOS.workflow()
    static async gatedWorkflow(input: string): Promise<string> {
      await SingletonTest.workflowEvent;
      return `${input}-done`;
    }

    @DBOS.workflow()
    static async parentWithSingleton(dedupID: string): Promise<string> {
      const handle = await DBOS.startWorkflow(SingletonTest, {
        queueName: SingletonTest.queue.name,
        enqueueOptions: { deduplicationID: dedupID },
        singleton: true,
      }).gatedWorkflow('attached');
      await DBOS.runStep(
        () => {
          SingletonTest.markerStepRuns++;
          return Promise.resolve('after-singleton');
        },
        { name: 'marker_step' },
      );
      return handle.workflowID;
    }
  }

  test('collision returns existing handle', async () => {
    const dedupID = 'collision_id';

    const wfh1 = await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID },
      singleton: true,
    }).gatedWorkflow('first');

    const wfh2 = await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID },
      singleton: true,
    }).gatedWorkflow('second');

    expect(wfh2.workflowID).toBe(wfh1.workflowID);

    SingletonTest.resolveEvent();

    const result1 = await wfh1.getResult();
    const result2 = await wfh2.getResult();
    expect(result1).toBe('first-done');
    expect(result2).toBe('first-done');
  });

  test('fresh enqueue after completion starts a new workflow', async () => {
    const dedupID = 'fresh_id';

    const wfh1 = await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID },
      singleton: true,
    }).gatedWorkflow('first');

    SingletonTest.resolveEvent();
    expect(await wfh1.getResult()).toBe('first-done');

    SingletonTest.resetEvent();

    const wfh2 = await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID },
      singleton: true,
    }).gatedWorkflow('second');

    expect(wfh2.workflowID).not.toBe(wfh1.workflowID);

    SingletonTest.resolveEvent();
    expect(await wfh2.getResult()).toBe('second-done');
  });

  test('missing deduplicationID throws', async () => {
    await expect(
      DBOS.startWorkflow(SingletonTest, {
        queueName: SingletonTest.queue.name,
        singleton: true,
      }).gatedWorkflow('x'),
    ).rejects.toThrow(/deduplicationID/);
  });

  test('missing queueName throws', async () => {
    await expect(
      DBOS.startWorkflow(SingletonTest, {
        enqueueOptions: { deduplicationID: 'some_id' },
        singleton: true,
      }).gatedWorkflow('y'),
    ).rejects.toThrow(/queueName/);
  });

  // Exercises the loop: the dedup_id was cleared between our failed INSERT
  // and the lookup (i.e. the prior workflow completed mid-flight). We force
  // the lookup to return null on its first call so the loop must retry.
  test('retries when dedup lookup races with prior completion', async () => {
    const dedupID = 'race_id';

    const wfh1 = await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID },
      singleton: true,
    }).gatedWorkflow('first');

    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const original = sysdb.getDeduplicatedWorkflow.bind(sysdb);
    let calls = 0;
    sysdb.getDeduplicatedWorkflow = async (queueName: string, dID: string) => {
      calls++;
      if (calls === 1) return null;
      return original(queueName, dID);
    };

    try {
      const wfh2 = await DBOS.startWorkflow(SingletonTest, {
        queueName: SingletonTest.queue.name,
        enqueueOptions: { deduplicationID: dedupID },
        singleton: true,
      }).gatedWorkflow('second');

      expect(calls).toBe(2);
      expect(wfh2.workflowID).toBe(wfh1.workflowID);

      SingletonTest.resolveEvent();
      expect(await wfh1.getResult()).toBe('first-done');
      expect(await wfh2.getResult()).toBe('first-done');
    } finally {
      sysdb.getDeduplicatedWorkflow = original;
    }
  });

  // Regression test: when called from inside a parent workflow, the singleton retry
  // loop must consume exactly one parent funcID, not one per iteration. Otherwise
  // subsequent operations land at funcIDs that don't match what replay expects.
  test('singleton from within a parent workflow consumes one funcID across retries', async () => {
    const dedupID = 'parent_singleton_id';

    // Pre-enqueue a child that holds the dedup slot.
    await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID },
      singleton: true,
    }).gatedWorkflow('blocking');

    SingletonTest.markerStepRuns = 0;

    // Force the dedup lookup to miss once so the retry loop iterates twice on the
    // original run.
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const original = sysdb.getDeduplicatedWorkflow.bind(sysdb);
    let calls = 0;
    sysdb.getDeduplicatedWorkflow = async (qn: string, dID: string) => {
      calls++;
      if (calls === 1) return null;
      return original(qn, dID);
    };

    let parentID: string;
    try {
      const parentHandle = await DBOS.startWorkflow(SingletonTest).parentWithSingleton(dedupID);
      await parentHandle.getResult();
      parentID = parentHandle.workflowID;
    } finally {
      sysdb.getDeduplicatedWorkflow = original;
    }

    expect(SingletonTest.markerStepRuns).toBe(1);

    const steps = await DBOS.listWorkflowSteps(parentID);
    // Singleton attach doesn't record a child mapping, so the parent's only
    // recorded operation is marker_step.
    expect(steps?.length).toBe(1);
    const markerStep = steps?.find((s) => s.name === 'marker_step');
    expect(markerStep).toBeDefined();
    // With the fix the singleton consumes one funcID (0) and marker_step lands
    // at functionID 1. Without the fix the retry burns an extra funcID and
    // marker_step lands at functionID 2.
    expect(markerStep!.functionID).toBe(1);

    // Fork past the final recorded step. Replay should reuse the cached
    // marker_step output rather than re-executing it. Without the funcID fix,
    // the original run recorded marker_step at functionID 2, but replay (no
    // mock) burns only one funcID for the singleton and probes functionID 1 —
    // missing the cache and re-running the step body, which would bump
    // markerStepRuns.
    const forkedHandle = await DBOS.forkWorkflow(parentID, markerStep!.functionID + 1);
    await forkedHandle.getResult();
    expect(SingletonTest.markerStepRuns).toBe(1);

    const forkedSteps = await DBOS.listWorkflowSteps(forkedHandle.workflowID);
    expect(forkedSteps?.length).toBe(1);
    const forkedMarkerStep = forkedSteps?.find((s) => s.name === 'marker_step');
    expect(forkedMarkerStep).toBeDefined();
    expect(forkedMarkerStep!.functionID).toBe(markerStep!.functionID);
  });
});
