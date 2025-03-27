import { DBOS } from '../src';
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

describe.only('pgnode-engine-config-tests', () => {
  test('engine-config', async () => {
    const config = generatePublicDBOSTestConfig({
      userDbclient: UserDatabaseName.PGNODE,
      userDbPoolSize: 2,
    });
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
    try {
      await TestEngine.testEngine();
    } finally {
      await DBOS.shutdown();
    }
  });
});
