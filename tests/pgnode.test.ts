import { DBOS } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { PostgresSystemDatabase } from '../src/system_database';
import { UserDatabaseName } from '../src/user_database';
import { generatePublicDBOSTestConfig, setUpDBOSTestDb } from './helpers';

class TestEngine {
  @DBOS.transaction()
  static async testEngine() {
    const ds = DBOS.pgClient;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
    expect((ds as any)._connectionTimeoutMillis).toEqual(3000);
    // PG doesn't expose the pool directly
    await Promise.resolve();
  }
}

describe('pgnode-engine-config-tests', () => {
  test('engine-config', async () => {
    const config = generatePublicDBOSTestConfig({
      userDbclient: UserDatabaseName.PGNODE,
      userDbPoolSize: 2,
      sysDbPoolSize: 42,
    });
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
    try {
      const sysDbClient = ((DBOS.executor as DBOSExecutor).systemDatabase as PostgresSystemDatabase).knexDB;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
      expect((sysDbClient as any).context.client.config.pool.max).toEqual(42);
      await TestEngine.testEngine();
    } finally {
      await DBOS.shutdown();
    }
  });
});
