import { Client } from 'pg';
import { DBOSJSON } from '../src/serialization';
import { SystemDatabase } from '../src/system_database';
import { allMigrations } from '../src/sysdb_migrations/internal/migrations';
import { runSysMigrationsPg } from '../src/sysdb_migrations/migration_runner';
import { GlobalLogger } from '../src/telemetry/logs';
import { ensureTestDatabase, generateDBOSTestConfig } from './helpers';

const SCHEMA = 'dbos_versions_index_test';

interface PlanNode {
  'Node Type': string;
  'Actual Rows': number;
  'Actual Loops': number;
  'Rows Removed by Filter'?: number;
  'Shared Hit Blocks': number;
  'Shared Read Blocks': number;
  Plans?: PlanNode[];
}

function planNodes(plan: PlanNode): PlanNode[] {
  return [plan, ...(plan.Plans ?? []).flatMap(planNodes)];
}

describe('indexed latest application version', () => {
  const config = generateDBOSTestConfig();
  let client: Client;

  beforeAll(async () => {
    await ensureTestDatabase(config.systemDatabaseUrl!);
    client = new Client({ connectionString: config.systemDatabaseUrl });
    await client.connect();
    await client.query(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
    await runSysMigrationsPg(client, allMigrations(SCHEMA, { useListenNotify: false }), SCHEMA, { onWarn: () => {} });
    // The newest third belongs to a peer, so a global timestamp scan with an owner filter is insufficient.
    await client.query(`
      INSERT INTO "${SCHEMA}".application_versions
        (version_id, version_name, version_timestamp, created_at, application_name)
      SELECT i::text, 'version-' || i, i, i,
             CASE WHEN i <= 10000 THEN 'ours' WHEN i <= 20000 THEN NULL ELSE 'peer' END
      FROM generate_series(1, 30000) AS i
    `);
    await client.query(`ANALYZE "${SCHEMA}".application_versions`);
  });

  afterAll(async () => {
    await client.query(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
    await client.end();
  });

  function systemDatabase(owner?: string): SystemDatabase {
    return new SystemDatabase(
      config.systemDatabaseUrl!,
      new GlobalLogger(),
      DBOSJSON,
      2,
      undefined,
      SCHEMA,
      false,
      undefined,
      undefined,
      owner,
    );
  }

  test.each([
    ['ours', 'version-20000'],
    ['absent', 'version-20000'],
    ['peer', 'version-30000'],
    [undefined, 'version-30000'],
    ['', 'version-30000'],
  ])('finds latest for %s without scanning version history', async (owner, expected) => {
    const sysdb = systemDatabase(owner);
    const queries = jest.spyOn(sysdb.pool, 'query');
    try {
      expect((await sysdb.getLatestApplicationVersion()).versionName).toBe(expected);
      const [sql, values] = queries.mock.calls[0];
      if (typeof sql !== 'string') throw new Error('Expected a SQL statement');
      const result = await client.query<{ 'QUERY PLAN': { Plan: PlanNode }[] }>(
        `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`,
        values as unknown[],
      );
      const plan = result.rows[0]['QUERY PLAN'][0].Plan;
      expect(plan['Shared Hit Blocks'] + plan['Shared Read Blocks']).toBeLessThan(32);
      const scans = planNodes(plan).filter((node) => node['Node Type'].includes('Scan'));
      expect(scans.length).toBeGreaterThan(0);
      for (const scan of scans) {
        expect(scan['Node Type']).toMatch(/Index/);
        expect(scan['Actual Rows'] * scan['Actual Loops']).toBeLessThanOrEqual(1);
        expect(scan['Rows Removed by Filter'] ?? 0).toBe(0);
      }
    } finally {
      queries.mockRestore();
      await sysdb.destroy();
    }
  });

  test('promotion and re-registration preserve version identity and creation time', async () => {
    const sysdb = systemDatabase('ours');
    try {
      await sysdb.updateApplicationVersionTimestamp('version-1', 40000);
      const promoted = await sysdb.getLatestApplicationVersion();
      expect(promoted).toMatchObject({
        versionId: '1',
        versionName: 'version-1',
        versionTimestamp: 40000,
        createdAt: 1,
        applicationName: 'ours',
      });
      await sysdb.createApplicationVersion('version-1');
      expect(await sysdb.getLatestApplicationVersion()).toEqual(promoted);
      // A named handle may explicitly look up a peer without changing its own scope.
      expect((await sysdb.getLatestApplicationVersion('peer')).versionName).toBe('version-30000');
    } finally {
      await sysdb.updateApplicationVersionTimestamp('version-1', 1);
      await sysdb.destroy();
    }
  });

  test('empty scope preserves the public missing-version error', async () => {
    const sysdb = systemDatabase('absent');
    try {
      await client.query(`DELETE FROM "${SCHEMA}".application_versions WHERE application_name IS NULL`);
      await expect(sysdb.getLatestApplicationVersion()).rejects.toThrow('No application versions found');
    } finally {
      await client.query(`
        INSERT INTO "${SCHEMA}".application_versions (version_id, version_name, version_timestamp, created_at)
        SELECT i::text, 'version-' || i, i, i FROM generate_series(10001, 20000) AS i
      `);
      await sysdb.destroy();
    }
  });
});
