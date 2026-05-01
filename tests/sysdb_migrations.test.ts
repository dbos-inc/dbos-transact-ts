import { Client } from 'pg';
import { DBMigration, getCurrentSysDBVersion, runSysMigrationsPg } from '../src/sysdb_migrations/migration_runner';
import { allMigrations } from '../src/sysdb_migrations/internal/migrations';
import { generateDBOSTestConfig } from './helpers';

const TEST_SCHEMA = 'dbos_migration_test';

async function getClient(): Promise<Client> {
  const config = generateDBOSTestConfig();
  const client = new Client({ connectionString: config.systemDatabaseUrl });
  await client.connect();
  return client;
}

async function isCockroach(client: Client): Promise<boolean> {
  const v = await client.query<{ version: string }>('SELECT version() AS version');
  return /cockroachdb/i.test(v.rows[0]?.version ?? '');
}

async function resetSchema(client: Client): Promise<void> {
  await client.query(`DROP SCHEMA IF EXISTS "${TEST_SCHEMA}" CASCADE`);
}

async function indexExists(client: Client, name: string): Promise<boolean> {
  const res = await client.query<{ exists: boolean }>(
    `SELECT EXISTS (
       SELECT 1 FROM pg_indexes
       WHERE schemaname = $1 AND indexname = $2
     ) AS exists`,
    [TEST_SCHEMA, name],
  );
  return res.rows[0].exists;
}

describe('sysdb migration runner', () => {
  let client: Client;
  let cockroach: boolean;

  beforeAll(async () => {
    client = await getClient();
    cockroach = await isCockroach(client);
  });

  beforeEach(async () => {
    await resetSchema(client);
  });

  afterAll(async () => {
    await resetSchema(client);
    await client.end();
  });

  test('idempotent re-run of full migration list', async () => {
    const migrations = allMigrations(TEST_SCHEMA, { useListenNotify: false, isCockroach: cockroach });

    const first = await runSysMigrationsPg(client, migrations, TEST_SCHEMA, {
      isCockroach: cockroach,
      onWarn: () => {},
    });
    expect(first.fromVersion).toBe(0);
    expect(first.toVersion).toBe(migrations.length);
    expect(first.appliedCount).toBeGreaterThan(0);

    const second = await runSysMigrationsPg(client, migrations, TEST_SCHEMA, {
      isCockroach: cockroach,
      onWarn: () => {},
    });
    expect(second.fromVersion).toBe(migrations.length);
    expect(second.toVersion).toBe(migrations.length);
    expect(second.appliedCount).toBe(0);

    // The new partial indexes should exist; the broad indexes they replace should not.
    expect(await indexExists(client, 'idx_workflow_status_pending')).toBe(true);
    expect(await indexExists(client, 'idx_workflow_status_failed')).toBe(true);
    expect(await indexExists(client, 'idx_workflow_status_in_flight')).toBe(true);
    expect(await indexExists(client, 'idx_workflow_status_rate_limited')).toBe(true);
    expect(await indexExists(client, 'uq_workflow_status_dedup_id')).toBe(true);

    expect(await indexExists(client, 'workflow_status_status_index')).toBe(false);
    expect(await indexExists(client, 'workflow_status_executor_id_index')).toBe(false);
    expect(await indexExists(client, 'idx_workflow_status_queue_status_started')).toBe(false);
  });

  test('per-version bump on partial failure resumes on retry', async () => {
    const baseSchema: ReadonlyArray<DBMigration> = [
      { pg: [`CREATE SCHEMA IF NOT EXISTS "${TEST_SCHEMA}"`] },
      {
        pg: [
          `CREATE TABLE "${TEST_SCHEMA}"."dbos_migrations" ("version" bigint not null, constraint "dbos_migrations_pkey_t" primary key ("version"))`,
        ],
      },
      { pg: [`CREATE TABLE "${TEST_SCHEMA}"."t1" (id int)`] },
    ];

    const failing: ReadonlyArray<DBMigration> = [
      ...baseSchema,
      { pg: [`SELECT 1/0`] }, // version 4 — fails at runtime
      { pg: [`CREATE TABLE "${TEST_SCHEMA}"."t2" (id int)`] }, // version 5 — never reached
    ];

    await expect(
      runSysMigrationsPg(client, failing, TEST_SCHEMA, { isCockroach: cockroach, onWarn: () => {} }),
    ).rejects.toBeDefined();

    expect(await getCurrentSysDBVersion(client, TEST_SCHEMA)).toBe(3);

    const fixed: ReadonlyArray<DBMigration> = [
      ...baseSchema,
      { pg: [`SELECT 1`] },
      { pg: [`CREATE TABLE "${TEST_SCHEMA}"."t2" (id int)`] },
    ];

    const result = await runSysMigrationsPg(client, fixed, TEST_SCHEMA, {
      isCockroach: cockroach,
      onWarn: () => {},
    });
    expect(result.fromVersion).toBe(3);
    expect(result.toVersion).toBe(5);
    expect(result.appliedCount).toBe(2);
  });

  test('cleans up invalid indexes left by an interrupted CONCURRENTLY build', async () => {
    if (cockroach) {
      // CockroachDB doesn't have the same INVALID-index recovery story.
      return;
    }

    await client.query(`CREATE SCHEMA "${TEST_SCHEMA}"`);
    await client.query(
      `CREATE TABLE "${TEST_SCHEMA}"."dbos_migrations" ("version" bigint not null, constraint "dbos_migrations_pkey_invalid" primary key ("version"))`,
    );
    await client.query(`CREATE TABLE "${TEST_SCHEMA}"."tgt" (id int)`);
    await client.query(`CREATE INDEX "tgt_invalid_idx" ON "${TEST_SCHEMA}"."tgt" (id)`);
    await client.query(
      `UPDATE pg_index SET indisvalid = false WHERE indexrelid = ('"${TEST_SCHEMA}"."tgt_invalid_idx"')::regclass`,
    );

    expect(await indexExists(client, 'tgt_invalid_idx')).toBe(true);

    const migrations: ReadonlyArray<DBMigration> = [
      { pg: [`SELECT 1`] }, // bumps to v1 (the schema/table already exist)
      { online: true, pg: [`CREATE INDEX CONCURRENTLY IF NOT EXISTS "tgt_good_idx" ON "${TEST_SCHEMA}"."tgt" (id)`] },
    ];

    await runSysMigrationsPg(client, migrations, TEST_SCHEMA, { isCockroach: false, onWarn: () => {} });

    expect(await indexExists(client, 'tgt_invalid_idx')).toBe(false);
    expect(await indexExists(client, 'tgt_good_idx')).toBe(true);
  });
});
