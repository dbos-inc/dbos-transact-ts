import { DBOS, DBOSConfig } from '../src/';
import { startDockerPg, stopDockerPg } from '../src/dbos-runtime/docker_pg_helper';
import { dropDatabases, PostgresChaosMonkey } from './helpers';

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
    const numWorkflows = 1000;
    for (let i = 0; i < numWorkflows; i++) {
      await expect(TestWorkflow.workflow(i))
        .resolves.toEqual(i + 3)
        .catch((err) => {
          console.error(`Workflow ${i} failed:`, err);
          console.error('Full error object:', JSON.stringify(err, null, 2));
          throw err;
        });
      console.log(i);
    }
  });
});
