import { DBOS, DBOSConfig } from '../src/';

describe('chaos-tests', () => {
  beforeAll(() => {
    const config: DBOSConfig = {
      name: 'test-app',
      databaseUrl: `postgresql://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/dbostest?sslmode=disable`,
    };
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  class TestWorkflow {
    static async step_one(x: number) {
      return Promise.resolve(x + 1);
    }

    static async step_two(x: number) {
      return Promise.resolve(x + 2);
    }
    static async workflow(x: number) {
      x = await TestWorkflow.step_one(x);
      x = await TestWorkflow.step_two(x);
      return x;
    }
  }

  test('test-workflow', async () => {
    const numWorkflows = 10;
    for (let i = 0; i < numWorkflows; i++) {
      await expect(TestWorkflow.workflow(i)).resolves.toEqual(i + 3);
    }
  });
});
