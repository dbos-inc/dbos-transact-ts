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
    static async parentWithSingleton(dedupID: string, childInput: string): Promise<string> {
      const handle = await DBOS.startWorkflow(SingletonTest, {
        queueName: SingletonTest.queue.name,
        enqueueOptions: { deduplicationID: dedupID },
        singleton: true,
      }).gatedWorkflow(childInput);
      const result = await handle.getResult();
      await DBOS.runStep(
        () => {
          SingletonTest.markerStepRuns++;
          return Promise.resolve('after-singleton');
        },
        { name: 'marker_step' },
      );
      return result;
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

    // First child holds the dedup slot. Subsequent attaches should resolve
    // to this child's output regardless of what input they passed.
    const firstChildHandle = await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID },
      singleton: true,
    }).gatedWorkflow('first');

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

    let parentAID: string;
    let parentBID: string;
    let resultA: string;
    let resultB: string;
    try {
      // Start two parents in flight with different child inputs. Both should
      // attach to the blocking child via the same dedup slot and end up
      // returning that child's output ('first-done'), not the input they
      // themselves passed.
      const parentA = await DBOS.startWorkflow(SingletonTest).parentWithSingleton(dedupID, 'second');
      const parentB = await DBOS.startWorkflow(SingletonTest).parentWithSingleton(dedupID, 'third');
      parentAID = parentA.workflowID;
      parentBID = parentB.workflowID;

      // Release the gate so the blocking child can complete; then both
      // parents' `await handle.getResult()` resolves.
      SingletonTest.resolveEvent();
      resultA = await parentA.getResult();
      resultB = await parentB.getResult();
    } finally {
      sysdb.getDeduplicatedWorkflow = original;
    }

    // Each parent attaches to the first child and returns its output.
    expect(resultA).toBe('first-done');
    expect(resultB).toBe('first-done');

    // marker_step ran exactly once per parent.
    expect(SingletonTest.markerStepRuns).toBe(2);

    const steps = await DBOS.listWorkflowSteps(parentAID);
    // Parent A records three operations: the singleton attach (childWorkflowID
    // points at the blocking child), the awaited child result, and marker_step.
    expect(steps?.length).toBe(3);
    const attachStep = steps?.find(
      (s) => s.name !== 'marker_step' && s.childWorkflowID === firstChildHandle.workflowID,
    );
    expect(attachStep).toBeDefined();
    // With the fix the singleton consumes one funcID (0). Without the fix the
    // retry burns an extra funcID and the attach lands later.
    expect(attachStep!.functionID).toBe(0);
    const markerStep = steps?.find((s) => s.name === 'marker_step');
    expect(markerStep).toBeDefined();
    expect(markerStep!.functionID).toBe(2);

    // Fork parent A past its final step. Replay should reuse all three cached
    // operations — singleton attach, awaited result, marker_step — rather than
    // re-executing them.
    const forkedHandle = await DBOS.forkWorkflow<string>(parentAID, markerStep!.functionID + 1);
    const forkedResult = await forkedHandle.getResult();
    expect(forkedResult).toBe('first-done');
    expect(SingletonTest.markerStepRuns).toBe(2);

    const forkedSteps = await DBOS.listWorkflowSteps(forkedHandle.workflowID);
    expect(forkedSteps?.length).toBe(3);
    const forkedAttachStep = forkedSteps?.find(
      (s) => s.name !== 'marker_step' && s.childWorkflowID === firstChildHandle.workflowID,
    );
    expect(forkedAttachStep).toBeDefined();
    expect(forkedAttachStep!.functionID).toBe(attachStep!.functionID);
    const forkedMarkerStep = forkedSteps?.find((s) => s.name === 'marker_step');
    expect(forkedMarkerStep).toBeDefined();
    expect(forkedMarkerStep!.functionID).toBe(markerStep!.functionID);

    // Sanity: parent B is not used in fork checks, but its output and step
    // count are still asserted via earlier resultB / markerStepRuns checks.
    expect(parentBID).toBeDefined();
  });
});
