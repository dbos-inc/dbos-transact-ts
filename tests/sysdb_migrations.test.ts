import { Client } from 'pg';
import { DBMigration, getCurrentSysDBVersion, runSysMigrationsPg } from '../src/sysdb_migrations/migration_runner';
import { allMigrations, SHARED_MIGRATION_BASE } from '../src/sysdb_migrations/internal/migrations';
import { generateDBOSTestConfig } from './helpers';

const TEST_SCHEMA = 'dbos_migration_test';

async function getClient(): Promise<Client> {
  const config = generateDBOSTestConfig();
  const client = new Client({ connectionString: config.systemDatabaseUrl });
  await client.connect();
  return client;
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

async function indexDefinition(client: Client, name: string): Promise<string | undefined> {
  const res = await client.query<{ indexdef: string }>(
    `SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND indexname = $2`,
    [TEST_SCHEMA, name],
  );
  return res.rows[0]?.indexdef;
}

describe('sysdb migration runner', () => {
  let client: Client;

  beforeAll(async () => {
    client = await getClient();
  });

  beforeEach(async () => {
    await resetSchema(client);
  });

  afterAll(async () => {
    await resetSchema(client);
    await client.end();
  });

  test('idempotent re-run of full migration list', async () => {
    const migrations = allMigrations(TEST_SCHEMA, { useListenNotify: false });

    const first = await runSysMigrationsPg(client, migrations, TEST_SCHEMA, {
      onWarn: () => {},
    });
    expect(first.fromVersion).toBe(0);
    expect(first.toVersion).toBe(migrations.length);
    expect(first.appliedCount).toBeGreaterThan(0);

    const second = await runSysMigrationsPg(client, migrations, TEST_SCHEMA, {
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
    expect(await indexExists(client, 'idx_workflow_status_partition_dequeue_v2')).toBe(true);
    // v2 must carry the workflow_uuid tiebreaker, which is what keeps the batched head probe index-provided.
    expect(await indexDefinition(client, 'idx_workflow_status_partition_dequeue_v2')).toContain(
      'priority, created_at, workflow_uuid',
    );

    expect(await indexExists(client, 'workflow_status_status_index')).toBe(false);
    expect(await indexExists(client, 'workflow_status_executor_id_index')).toBe(false);
    expect(await indexExists(client, 'idx_workflow_status_queue_status_started')).toBe(false);
    // Superseded by v2, so the original name must be gone.
    expect(await indexExists(client, 'idx_workflow_status_partition_dequeue')).toBe(false);
  });

  // application_name is nullable everywhere it appears — NULL is what SDKs predating it
  // write — and every name that addresses a row stays globally unique.
  test('shared migrations add nullable application_name and the ownership keys', async () => {
    const migrations = allMigrations(TEST_SCHEMA, { useListenNotify: false });
    await runSysMigrationsPg(client, migrations, TEST_SCHEMA, { onWarn: () => {} });

    const tables = ['workflow_status', 'queues', 'workflow_schedules', 'application_versions', 'operation_outputs'];
    for (const table of tables) {
      const res = await client.query<{ data_type: string; is_nullable: string }>(
        `SELECT data_type, is_nullable FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2 AND column_name = 'application_name'`,
        [TEST_SCHEMA, table],
      );
      expect(res.rows[0]).toEqual({ data_type: 'text', is_nullable: 'YES' });
    }

    // Names stay global addresses; version_name only until the contract migration,
    // whose replacement key migration 106 already carries.
    for (const index of [
      'uq_workflow_status_dedup_id',
      'queues_name_key',
      'workflow_schedules_schedule_name_key',
      'application_versions_version_name_key',
      'uq_application_versions_owner_version',
      'uq_application_versions_unclaimed_version',
    ]) {
      expect(await indexExists(client, index)).toBe(true);
    }

    // enqueue_workflow gained a trailing application_name parameter.
    const fn = await client.query<{ args: string }>(
      `SELECT pg_get_function_identity_arguments(p.oid) AS args
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1 AND p.proname = 'enqueue_workflow'`,
      [TEST_SCHEMA],
    );
    expect(fn.rows).toHaveLength(1);
    expect(fn.rows[0].args.endsWith('text')).toBe(true);
    expect(fn.rows[0].args.split(',').length).toBe(17);
  });

  // Renumbering onto the shared base leaves long runs of empty migrations.
  test('padding to the shared base still lands on the true migration count', async () => {
    const migrations = allMigrations(TEST_SCHEMA, { useListenNotify: false });
    expect(migrations.length).toBeGreaterThan(SHARED_MIGRATION_BASE);
    expect(migrations.filter((m) => (m.pg ?? []).length === 0).length).toBeGreaterThan(0);

    const result = await runSysMigrationsPg(client, migrations, TEST_SCHEMA, { onWarn: () => {} });
    expect(result.toVersion).toBe(migrations.length);
    expect(await getCurrentSysDBVersion(client, TEST_SCHEMA)).toBe(migrations.length);
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

    await expect(runSysMigrationsPg(client, failing, TEST_SCHEMA, { onWarn: () => {} })).rejects.toBeDefined();

    expect(await getCurrentSysDBVersion(client, TEST_SCHEMA)).toBe(3);

    const fixed: ReadonlyArray<DBMigration> = [
      ...baseSchema,
      { pg: [`SELECT 1`] },
      { pg: [`CREATE TABLE "${TEST_SCHEMA}"."t2" (id int)`] },
    ];

    const result = await runSysMigrationsPg(client, fixed, TEST_SCHEMA, {
      onWarn: () => {},
    });
    expect(result.fromVersion).toBe(3);
    expect(result.toVersion).toBe(5);
    expect(result.appliedCount).toBe(2);
  });

  test('cleans up invalid indexes left by an interrupted CONCURRENTLY build', async () => {
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

    await runSysMigrationsPg(client, migrations, TEST_SCHEMA, { onWarn: () => {} });

    expect(await indexExists(client, 'tgt_invalid_idx')).toBe(false);
    expect(await indexExists(client, 'tgt_good_idx')).toBe(true);
  });
});
