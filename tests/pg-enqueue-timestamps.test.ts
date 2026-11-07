import { Client } from 'pg';
import { allMigrations } from '../src/sysdb_migrations/internal/migrations';
import { runSysMigrationsPg } from '../src/sysdb_migrations/migration_runner';
import { describeUnlessSharded, ensureTestDatabase, generateDBOSTestConfig } from './helpers';

const schema = 'dbos_enqueue_timestamps_test';
const enqueueSql = `SELECT "${schema}".enqueue_workflow(
  workflow_name => 'timestampTest', queue_name => 'timestamp-queue',
  workflow_id => $1, positional_args => ARRAY[$2::json])`;

type Timestamps = { created_at: string; updated_at: string; retention_timestamp: string; inputs: string };

// These tests keep workers stopped: both rows must be visible before comparing queue positions.
describeUnlessSharded('PostgreSQL enqueue timestamps', () => {
  let observer: Client;
  let first: Client;
  let second: Client;

  beforeAll(async () => {
    const connectionString = generateDBOSTestConfig().systemDatabaseUrl;
    if (!connectionString) throw new Error('Missing test system database URL');
    await ensureTestDatabase(connectionString);
    observer = new Client({ connectionString });
    first = new Client({ connectionString });
    second = new Client({ connectionString });
    await Promise.all([observer.connect(), first.connect(), second.connect()]);
    await first.query("SET statement_timeout = '10s'");
    await second.query("SET statement_timeout = '10s'");
  });

  beforeEach(async () => {
    await observer.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await runSysMigrationsPg(observer, allMigrations(schema, { useListenNotify: false }), schema);
  });

  afterEach(async () => {
    await Promise.all([first.query('ROLLBACK'), second.query('ROLLBACK')]);
  });

  afterAll(async () => {
    await observer.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await Promise.all([observer.end(), first.end(), second.end()]);
  });

  const enqueue = (client: Client, id: string, value: string = id) =>
    client.query(enqueueSql, [id, JSON.stringify(value)]);
  const timestamps = async (id: string) => {
    const { rows } = await observer.query<Timestamps>(
      `SELECT s.created_at, s.updated_at, i.retention_timestamp, i.inputs
       FROM "${schema}".workflow_status s JOIN "${schema}".workflow_input i USING (workflow_uuid)
       WHERE workflow_uuid = $1`,
      [id],
    );
    return rows[0];
  };

  test('an older transaction does not backdate a later enqueue', async () => {
    await first.query('BEGIN');
    await first.query('SELECT now()');
    await observer.query('SELECT pg_sleep(0.02)');
    await enqueue(second, 'earlier-enqueue');
    await observer.query('SELECT pg_sleep(0.02)');
    await enqueue(first, 'later-enqueue');
    await first.query('COMMIT');

    const earlier = await timestamps('earlier-enqueue');
    const later = await timestamps('later-enqueue');
    expect(Number(later.created_at)).toBeGreaterThan(Number(earlier.created_at));
    expect(later.updated_at).toBe(later.created_at);
    expect(later.retention_timestamp).toBe(later.created_at);
  });

  test('successive enqueues within one statement sample the time independently', async () => {
    await first.query(`DO $$ BEGIN
      PERFORM "${schema}".enqueue_workflow('timestampTest', 'timestamp-queue', workflow_id => 'first');
      PERFORM pg_sleep(0.02);
      PERFORM "${schema}".enqueue_workflow('timestampTest', 'timestamp-queue', workflow_id => 'second');
    END $$`);
    expect(Number((await timestamps('second')).created_at)).toBeGreaterThan(
      Number((await timestamps('first')).created_at),
    );
  });

  test('an enqueue in a trigger is timestamped after the calling statement waits for a row lock', async () => {
    await observer.query(`
      CREATE TABLE "${schema}".source (id int PRIMARY KEY, label text);
      INSERT INTO "${schema}".source VALUES (1, 'initial');
      CREATE FUNCTION "${schema}".enqueue_update() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM "${schema}".enqueue_workflow('timestampTest', 'timestamp-queue', workflow_id => NEW.label);
        RETURN NEW;
      END $$;
      CREATE TRIGGER enqueue_update AFTER UPDATE ON "${schema}".source
        FOR EACH ROW EXECUTE FUNCTION "${schema}".enqueue_update();
    `);
    await second.query('BEGIN');
    await second.query(`SELECT * FROM "${schema}".source WHERE id = 1 FOR UPDATE`);
    const { rows: pidRows } = await first.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
    const waitingUpdate = first.query(`UPDATE "${schema}".source SET label = 'after-wait' WHERE id = 1`).then(
      () => undefined,
      (error: unknown) => error,
    );

    let updateError: unknown;
    try {
      // Observe the actual lock wait, rather than assuming a sleep was enough to start the UPDATE.
      const deadline = Date.now() + 5000;
      let blocked = false;
      while (Date.now() < deadline) {
        const { rows } = await observer.query<{ blocked: boolean }>(
          'SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked',
          [pidRows[0].pid],
        );
        if (rows[0].blocked) {
          blocked = true;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      expect(blocked).toBe(true);
      // This UPDATE starts later, but reaches its trigger first because it already owns the row lock.
      await second.query(`UPDATE "${schema}".source SET label = 'before-wait' WHERE id = 1`);
      await second.query('SELECT pg_sleep(0.02)');
    } finally {
      await second.query('COMMIT');
      updateError = await waitingUpdate;
    }
    expect(updateError).toBeUndefined();

    expect(Number((await timestamps('after-wait')).created_at)).toBeGreaterThan(
      Number((await timestamps('before-wait')).created_at),
    );
  });

  test('re-enqueue updates the timestamp without moving the workflow or replacing its input', async () => {
    await first.query('BEGIN');
    await enqueue(first, 'duplicate', 'original');
    await first.query('COMMIT');
    const original = await timestamps('duplicate');
    await first.query('BEGIN');
    await first.query('SELECT pg_sleep(0.02)');
    await enqueue(first, 'duplicate', 'replacement');
    await first.query('COMMIT');
    const repeated = await timestamps('duplicate');
    expect(repeated.created_at).toBe(original.created_at);
    expect(repeated.retention_timestamp).toBe(original.retention_timestamp);
    expect(repeated.inputs).toBe(original.inputs);
    expect(Number(repeated.updated_at)).toBeGreaterThan(Number(original.updated_at));
  });

  test('enqueue still rolls back atomically with its caller', async () => {
    await first.query('BEGIN');
    await enqueue(first, 'rolled-back');
    expect(await timestamps('rolled-back')).toBeUndefined();
    await first.query('ROLLBACK');
    expect(await timestamps('rolled-back')).toBeUndefined();
    for (const table of ['workflow_status', 'workflow_input']) {
      const result = await observer.query(`SELECT * FROM "${schema}".${table} WHERE workflow_uuid = 'rolled-back'`);
      expect(result.rowCount).toBe(0);
    }
  });
});
