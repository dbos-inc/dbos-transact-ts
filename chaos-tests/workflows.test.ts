import { DBOS, DBOSConfig } from '../src/';
import { startDockerPg, stopDockerPg } from '../src/dbos-runtime/docker_pg_helper';
import { Client } from 'pg';

describe('chaos-tests', () => {
  let config: DBOSConfig;
  jest.setTimeout(30000);

  beforeAll(() => {
    config = {
      name: 'test-app',
      databaseUrl: `postgresql://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/dbostest?sslmode=disable`,
    };
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await startDockerPg();
    await dropDatabases();
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
    await stopDockerPg();
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
    const numWorkflows = 10;
    for (let i = 0; i < numWorkflows; i++) {
      await expect(TestWorkflow.workflow(i)).resolves.toEqual(i + 3);
    }
  });
});
