import { DBOS } from '../src/';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, retryUntilSuccess, Event } from './helpers';
import { sleepConfig, sleepms } from '../src/utils';
import { DBOSError } from '../src/error';
import type { Conductor } from '../src/conductor/conductor';

// Every wait in this file is bounded: an unbounded one outlives the jest timeout that fails the test.
const WAIT_TIMEOUT_MS = 10000;

async function waitFor(event: Event, what: string) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      event.wait(),
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`Timed out waiting for ${what}`)), WAIT_TIMEOUT_MS);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

const allReleases: Event[] = [];

/** A workflow that parks until released, so a shutdown can be observed while it runs. */
function blockingWorkflow(name: string) {
  const started = new Event();
  const release = new Event();
  const state = { done: false };
  allReleases.push(release);
  const workflow = DBOS.registerWorkflow(
    async () => {
      started.set();
      await release.wait();
      state.done = true;
    },
    { name },
  );
  return { workflow, started, release, state };
}

const drained = blockingWorkflow('shutdownTimeoutBlockingWorkflow');
const conductorOrder = blockingWorkflow('shutdownConductorOrderWorkflow');
const defaultNoWait = blockingWorkflow('shutdownDefaultNoWaitWorkflow');
const zeroNoWait = blockingWorkflow('shutdownZeroTimeoutWorkflow');
const relaunchGuard = blockingWorkflow('shutdownRelaunchGuardWorkflow');

const childStarted = new Event();
const releaseChild = new Event();
let childFinished = false;

const childWorkflow = DBOS.registerWorkflow(
  async () => {
    childStarted.set();
    await releaseChild.wait();
    childFinished = true;
  },
  { name: 'shutdownTimeoutChildWorkflow' },
);

const parentStarted = new Event();
const releaseParent = new Event();

const parentWorkflow = DBOS.registerWorkflow(
  async () => {
    parentStarted.set();
    await releaseParent.wait();
    await DBOS.startWorkflow(childWorkflow)();
  },
  { name: 'shutdownTimeoutParentWorkflow' },
);

describe('shutdown-workflow-completion-timeout', () => {
  let config: DBOSConfig;
  // A shutdown a test started but may not have finished, so cleanup settles it before the next launch.
  let pendingShutdown: Promise<void> | undefined;
  let pendingShutdownError: Error | undefined;

  // Returns the raw promise so a caller awaiting it still sees a rejection; afterEach surfaces it otherwise.
  function trackShutdown(promise: Promise<void>): Promise<void> {
    pendingShutdown = promise.catch((e: unknown) => {
      pendingShutdownError = e instanceof Error ? e : new Error(String(e));
    });
    return promise;
  }

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  // These tests deliberately abandon workflows, and a PENDING row left behind would be recovered
  // (and re-run) by the next test's launch, so each one gets a fresh system database.
  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
  });

  afterEach(async () => {
    if (pendingShutdown) {
      await pendingShutdown;
      pendingShutdown = undefined;
    }
    DBOS.conductor = undefined;
    // Not isInitialized(): shutdown clears that before the drain, so a shutdown that failed
    // partway leaves an executor here that still needs closing.
    if (DBOSExecutor.globalInstance) {
      await DBOS.shutdown();
    }
    const failure = pendingShutdownError;
    pendingShutdownError = undefined;
    if (failure) throw failure;
  });

  afterAll(() => {
    for (const release of allReleases) release.set();
    releaseParent.set();
    releaseChild.set();
  });

  test('shutdown-waits-for-workflow-then-times-out', async () => {
    await DBOS.launch();
    const handle = await DBOS.startWorkflow(drained.workflow)();
    await waitFor(drained.started, 'the blocking workflow to start');

    try {
      const startTime = Date.now();
      await DBOS.shutdown({ workflowCompletionTimeoutMS: 1000 });

      expect(Date.now() - startTime).toBeGreaterThanOrEqual(900);
      expect(drained.state.done).toBe(false);
    } finally {
      drained.release.set();
      await handle.getResult().catch(() => {});
    }
  }, 20000);

  test('shutdown-waits-for-a-child-started-during-the-drain', async () => {
    await DBOS.launch();
    await DBOS.startWorkflow(parentWorkflow)();
    await waitFor(parentStarted, 'the parent workflow to start');

    // Set in the same turn as the drain's first snapshot of the running workflows, so
    // releasing the parent afterwards guarantees the child registers later.
    let drainStarted = false;
    const executor = DBOSExecutor.globalInstance!;
    const drainSpy = jest.spyOn(executor, 'awaitRunningWorkflows').mockImplementation(async (timeoutMS?: number) => {
      drainStarted = true;
      await executor.systemDatabase.awaitRunningWorkflows(timeoutMS);
    });

    try {
      let shutdownDone = false;
      const shutdownPromise = trackShutdown(
        DBOS.shutdown({ workflowCompletionTimeoutMS: 10000 }).then(() => {
          shutdownDone = true;
        }),
      );

      await retryUntilSuccess(() => {
        expect(drainStarted).toBe(true);
      });
      releaseParent.set();
      await waitFor(childStarted, 'the child workflow to start');
      await sleepms(500);
      expect(shutdownDone).toBe(false);
      expect(childFinished).toBe(false);

      releaseChild.set();
      await shutdownPromise;
      expect(childFinished).toBe(true);
    } finally {
      drainSpy.mockRestore();
      releaseParent.set();
      releaseChild.set();
    }
  }, 20000);

  test('conductor-disconnects-after-the-drain', async () => {
    await DBOS.launch();
    await DBOS.startWorkflow(conductorOrder.workflow)();
    await waitFor(conductorOrder.started, 'the blocking workflow to start');

    let stopSawWorkflowDone: boolean | undefined = undefined;
    const fakeConductor = {
      isClosed: false,
      stop() {
        stopSawWorkflowDone = conductorOrder.state.done;
        this.isClosed = true;
      },
      // No retention round to wait for.
      awaitRetention: () => Promise.resolve(),
    };
    DBOS.conductor = fakeConductor as unknown as Conductor;

    const shutdownPromise = trackShutdown(DBOS.shutdown({ workflowCompletionTimeoutMS: 10000 }));
    try {
      await sleepms(500);
      expect(stopSawWorkflowDone).toBeUndefined();
    } finally {
      conductorOrder.release.set();
      await shutdownPromise;
    }
    expect(stopSawWorkflowDone).toBe(true);
    expect(DBOS.conductor).toBeUndefined();
  }, 20000);

  test('shutdown-does-not-wait-when-no-timeout-is-given', async () => {
    await DBOS.launch();
    const handle = await DBOS.startWorkflow(defaultNoWait.workflow)();
    await waitFor(defaultNoWait.started, 'the blocking workflow to start');

    try {
      await DBOS.shutdown();
      expect(defaultNoWait.state.done).toBe(false);
    } finally {
      defaultNoWait.release.set();
      await handle.getResult().catch(() => {});
    }
  }, 20000);

  test('a-zero-timeout-does-not-wait', async () => {
    await DBOS.launch();
    const handle = await DBOS.startWorkflow(zeroNoWait.workflow)();
    await waitFor(zeroNoWait.started, 'the blocking workflow to start');

    try {
      await DBOS.shutdown({ workflowCompletionTimeoutMS: 0 });
      expect(zeroNoWait.state.done).toBe(false);
    } finally {
      zeroNoWait.release.set();
      await handle.getResult().catch(() => {});
    }
  }, 20000);

  test('an-invalid-timeout-is-rejected-and-leaves-dbos-running', async () => {
    await DBOS.launch();

    for (const invalid of [-1, Number.NaN, Number.POSITIVE_INFINITY, sleepConfig.maxTimeoutMS + 1]) {
      await expect(DBOS.shutdown({ workflowCompletionTimeoutMS: invalid })).rejects.toThrow(DBOSError);
      expect(DBOS.isInitialized()).toBe(true);
    }
  }, 20000);

  test('launch-is-refused-while-a-shutdown-is-in-flight', async () => {
    await DBOS.launch();
    const executor = DBOSExecutor.globalInstance;
    const handle = await DBOS.startWorkflow(relaunchGuard.workflow)();
    await waitFor(relaunchGuard.started, 'the blocking workflow to start');

    const shutdownPromise = trackShutdown(DBOS.shutdown({ workflowCompletionTimeoutMS: 10000 }));
    try {
      await expect(DBOS.launch()).rejects.toThrow(DBOSError);
      // The refused launch must not have swapped in an executor for the drain to tear down instead.
      expect(DBOSExecutor.globalInstance).toBe(executor);
    } finally {
      relaunchGuard.release.set();
      await shutdownPromise;
      await handle.getResult().catch(() => {});
    }
    expect(DBOSExecutor.globalInstance).toBeUndefined();
  }, 20000);
});
