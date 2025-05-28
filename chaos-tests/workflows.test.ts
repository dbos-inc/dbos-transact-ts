import { DBOS, DBOSConfig } from '../src/';
import { Client } from 'pg';

describe('chaos-tests', () => {
  let config: DBOSConfig;

  beforeAll(() => {
    config = {
      name: 'test-app',
      databaseUrl: `postgresql://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/dbostest?sslmode=disable`,
    };
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    // Drop databases before each test
    await dropDatabases();
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  async function dropDatabases() {
    const dbUrl = new URL(config.databaseUrl as string);
    const baseConnectionConfig = {
      host: dbUrl.hostname,
      port: parseInt(dbUrl.port) || 5432,
      user: dbUrl.username,
      password: dbUrl.password,
    };

    const adminClient = new Client({
      ...baseConnectionConfig,
      database: 'postgres',
    });

    try {
      await adminClient.connect();
      const dbName = 'dbostest';
      const dbNameSys = `${dbName}_dbos_sys`;
      await adminClient.query(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`);
      await adminClient.query(`DROP DATABASE IF EXISTS "${dbNameSys}" WITH (FORCE)`);
    } finally {
      await adminClient.end();
    }
  }

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
