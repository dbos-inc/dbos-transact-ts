import { DBOS } from '../src/';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, Event } from './helpers';

const started = new Event();
const release = new Event();

const blockingWorkflow = DBOS.registerWorkflow(
  async () => {
    started.set();
    await release.wait();
  },
  { name: 'shutdownTimeoutBlockingWorkflow' },
);

describe('shutdown-workflow-completion-timeout', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  afterAll(() => {
    release.set();
  });

  test('shutdown-waits-for-workflow-then-times-out', async () => {
    await DBOS.launch();
    const handle = await DBOS.startWorkflow(blockingWorkflow)();
    await started.wait();

    const startTime = Date.now();
    await DBOS.shutdown({ workflowCompletionTimeoutSec: 1 });
    const elapsed = Date.now() - startTime;

    expect(elapsed).toBeGreaterThanOrEqual(900);
    expect(elapsed).toBeLessThan(10000);

    release.set();
    await handle.getResult().catch(() => {});
  }, 20000);

  test('shutdown-returns-when-workflows-complete-before-timeout', async () => {
    await DBOS.launch();
    const startTime = Date.now();
    await DBOS.shutdown({ workflowCompletionTimeoutSec: 30 });
    expect(Date.now() - startTime).toBeLessThan(10000);
  }, 20000);
});
