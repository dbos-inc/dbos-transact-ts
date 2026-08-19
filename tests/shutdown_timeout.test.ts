import { DBOS } from '../src/';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, Event } from './helpers';
import { sleepms } from '../src/utils';
import { Conductor } from '../src/conductor/conductor';

const started = new Event();
const release = new Event();

const blockingWorkflow = DBOS.registerWorkflow(
  async () => {
    started.set();
    await release.wait();
  },
  { name: 'shutdownTimeoutBlockingWorkflow' },
);

const started2 = new Event();
const release2 = new Event();
let workflow2Done = false;

const blockingWorkflow2 = DBOS.registerWorkflow(
  async () => {
    started2.set();
    await release2.wait();
    workflow2Done = true;
  },
  { name: 'shutdownConductorOrderWorkflow' },
);

const started3 = new Event();
const release3 = new Event();
let workflow3Done = false;

const blockingWorkflow3 = DBOS.registerWorkflow(
  async () => {
    started3.set();
    await release3.wait();
    workflow3Done = true;
  },
  { name: 'shutdownDefaultNoWaitWorkflow' },
);

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

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  afterEach(async () => {
    if (pendingShutdown) {
      await pendingShutdown;
      pendingShutdown = undefined;
    }
    DBOS.conductor = undefined;
    if (DBOS.isInitialized()) {
      await DBOS.shutdown();
    }
  });

  afterAll(() => {
    release.set();
    release2.set();
    release3.set();
    releaseParent.set();
    releaseChild.set();
  });

  test('shutdown-waits-for-workflow-then-times-out', async () => {
    await DBOS.launch();
    const handle = await DBOS.startWorkflow(blockingWorkflow)();
    await started.wait();

    try {
      const startTime = Date.now();
      await DBOS.shutdown({ workflowCompletionTimeoutMS: 1000 });
      const elapsed = Date.now() - startTime;

      expect(elapsed).toBeGreaterThanOrEqual(900);
      expect(elapsed).toBeLessThan(10000);
    } finally {
      release.set();
      await handle.getResult().catch(() => {});
    }
  }, 20000);

  test('shutdown-waits-for-a-child-started-during-the-drain', async () => {
    await DBOS.launch();
    await DBOS.startWorkflow(parentWorkflow)();
    await parentStarted.wait();

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
      const shutdownPromise = DBOS.shutdown({ workflowCompletionTimeoutMS: 10000 }).then(() => {
        shutdownDone = true;
      });
      pendingShutdown = shutdownPromise.catch(() => {});

      while (!drainStarted) {
        await sleepms(10);
      }
      releaseParent.set();
      await childStarted.wait();
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
    await DBOS.startWorkflow(blockingWorkflow2)();
    await started2.wait();

    let stopSawWorkflowDone: boolean | undefined = undefined;
    const fakeConductor = {
      isClosed: false,
      stop() {
        stopSawWorkflowDone = workflow2Done;
        this.isClosed = true;
      },
    };
    DBOS.conductor = fakeConductor as unknown as Conductor;

    const shutdownPromise = DBOS.shutdown({ workflowCompletionTimeoutMS: 10000 });
    pendingShutdown = shutdownPromise.catch(() => {});
    try {
      await sleepms(500);
      expect(stopSawWorkflowDone).toBeUndefined();
    } finally {
      release2.set();
      await shutdownPromise;
    }
    expect(stopSawWorkflowDone).toBe(true);
    expect(DBOS.conductor).toBeUndefined();
  }, 20000);

  test('shutdown-does-not-wait-when-no-timeout-is-given', async () => {
    await DBOS.launch();
    const handle = await DBOS.startWorkflow(blockingWorkflow3)();
    await started3.wait();

    try {
      const startTime = Date.now();
      await DBOS.shutdown();
      expect(Date.now() - startTime).toBeLessThan(10000);
      expect(workflow3Done).toBe(false);
    } finally {
      release3.set();
      await handle.getResult().catch(() => {});
    }
  }, 20000);
});
