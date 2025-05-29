import { randomUUID } from 'crypto';
import { DBOS, DBOSConfig } from '../src/';
import { startDockerPg, stopDockerPg } from '../src/dbos-runtime/docker_pg_helper';
import { dropDatabases, PostgresChaosMonkey } from './helpers';
import { sleepms } from '../src/utils';

describe('chaos-tests', () => {
  let config: DBOSConfig;
  let chaosMonkey: PostgresChaosMonkey;

  jest.setTimeout(300000);

  beforeAll(() => {
    config = {
      name: 'test-app',
      databaseUrl: `postgresql://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/dbostest?sslmode=disable`,
    };
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await startDockerPg();
    await dropDatabases(config);
    await DBOS.launch();

    // Start chaos monkey after setup
    chaosMonkey = new PostgresChaosMonkey();
    chaosMonkey.start();
  });

  afterEach(async () => {
    // Stop chaos monkey before teardown
    if (chaosMonkey) {
      chaosMonkey.stop();
    }

    await DBOS.shutdown();
    await stopDockerPg();
  });

  class TestWorkflow {
    @DBOS.step()
    static async stepOne(x: number) {
      return Promise.resolve(x + 1);
    }

    @DBOS.step()
    static async stepTwo(x: number) {
      return Promise.resolve(x + 2);
    }

    @DBOS.workflow()
    static async workflow(x: number) {
      x = await TestWorkflow.stepOne(x);
      x = await TestWorkflow.stepTwo(x);
      return x;
    }
  }

  test('test-workflow', async () => {
    const numWorkflows = 5000;
    for (let i = 0; i < numWorkflows; i++) {
      await expect(TestWorkflow.workflow(i))
        .resolves.toEqual(i + 3)
        .catch((err) => {
          console.error(`Workflow ${i} failed:`, err);
          console.error('Full error object:', JSON.stringify(err, null, 2));
          throw err;
        });
      DBOS.logger.info(i);
    }
  });

  class TestRecv {
    static topic = 'test_topic';

    @DBOS.workflow()
    static async recvWorkflow() {
      return DBOS.recv(TestRecv.topic, 10);
    }
  }

  test('test-recv', async () => {
    const numWorkflows = 5000;
    for (let i = 0; i < numWorkflows; i++) {
      const handle = await DBOS.startWorkflow(TestRecv).recvWorkflow();
      const value = String(randomUUID());
      await DBOS.send(handle.workflowID, value, TestRecv.topic);
      await expect(handle.getResult()).resolves.toEqual(value);
      DBOS.logger.info(i);
    }
  });

  class TestEvents {
    static key = 'test_key';

    @DBOS.workflow()
    static async eventWorkflow() {
      const value = String(randomUUID());
      await DBOS.setEvent(TestEvents.key, value);
      return value;
    }
  }

  test('test-events', async () => {
    const numWorkflows = 5000;
    for (let i = 0; i < numWorkflows; i++) {
      const handle = await DBOS.startWorkflow(TestEvents).eventWorkflow();
      const value = await handle.getResult();
      await expect(DBOS.getEvent(handle.workflowID, TestEvents.key, 0)).resolves.toEqual(value);
      DBOS.logger.info(i);
    }
  });

  class TestScheduled {
    static value = 0;

    @DBOS.workflow()
    @DBOS.scheduled({ crontab: '* * * * * *' })
    static async increment(scheduled: Date, _actual: Date) {
      console.log(`Running scheduled workflow at ${String(scheduled)}`);
      TestScheduled.value++;
      return Promise.resolve();
    }
  }

  test('test-scheduled', async () => {
    await sleepms(120000);
    expect(TestScheduled.value).toBeGreaterThan(60);
  });
});
