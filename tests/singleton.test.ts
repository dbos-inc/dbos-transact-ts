import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
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
  }

  test('collision returns existing handle', async () => {
    const dedupID = 'collision_id';

    const wfh1 = await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID, singleton: true },
    }).gatedWorkflow('first');

    const wfh2 = await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID, singleton: true },
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
      enqueueOptions: { deduplicationID: dedupID, singleton: true },
    }).gatedWorkflow('first');

    SingletonTest.resolveEvent();
    expect(await wfh1.getResult()).toBe('first-done');

    SingletonTest.resetEvent();

    const wfh2 = await DBOS.startWorkflow(SingletonTest, {
      queueName: SingletonTest.queue.name,
      enqueueOptions: { deduplicationID: dedupID, singleton: true },
    }).gatedWorkflow('second');

    expect(wfh2.workflowID).not.toBe(wfh1.workflowID);

    SingletonTest.resolveEvent();
    expect(await wfh2.getResult()).toBe('second-done');
  });

  test('missing deduplicationID throws', async () => {
    await expect(
      DBOS.startWorkflow(SingletonTest, {
        queueName: SingletonTest.queue.name,
        enqueueOptions: { singleton: true },
      }).gatedWorkflow('x'),
    ).rejects.toThrow(/deduplicationID/);
  });

  test('missing queueName throws', async () => {
    await expect(
      DBOS.startWorkflow(SingletonTest, {
        enqueueOptions: { deduplicationID: 'some_id', singleton: true },
      }).gatedWorkflow('y'),
    ).rejects.toThrow(/queueName/);
  });
});
