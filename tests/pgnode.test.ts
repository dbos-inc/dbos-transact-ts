import { DBOSConfig } from '../dist/src';
import { DBOS } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { PostgresSystemDatabase } from '../src/system_database';
import { UserDatabaseName } from '../src/user_database';
import { setUpDBOSTestDb } from './helpers';

class TestEngine {
  @DBOS.transaction()
  static async testEngine() {
    const ds = DBOS.pgClient;

    // PG doesn't expose the pool directly
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
    const client = (ds as any).connectionParameters as { host: string; port: number; database: string; user: string };
    expect(client.database).toBe('dbostest');
    expect(client.user).toBe('postgres');
    expect(client.host).toBe('localhost');
    expect(client.port).toBe(5432);

    await Promise.resolve();
  }
}

describe('pgnode-engine-config-tests', () => {
  test('engine-config', async () => {
    const config: DBOSConfig = {
      userDatabaseClient: UserDatabaseName.PGNODE,
      userDatabasePoolSize: 2,
      systemDatabasePoolSize: 42,
      databaseUrl: `postgres://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/dbostest?connect_timeout=7`,
    };
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
    try {
      const url = new URL(config.databaseUrl!);
      url.pathname = '/dbostest_dbos_sys';
      const sysDbClient = (DBOSExecutor.globalInstance!.systemDatabase as PostgresSystemDatabase).knexDB;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
      expect((sysDbClient as any).context.client.connectionSettings.connectionString).toEqual(url.toString());
      await TestEngine.testEngine();
    } finally {
      await DBOS.shutdown();
    }
  });
});
