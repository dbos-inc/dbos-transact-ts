import { Client } from 'pg';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import net from 'net';

import { dropPGDatabase, ensurePGDatabase } from '../src/datasource';
import { spawn } from 'child_process';

// Test routine to see if psql can contact container
async function execHostPsql(
  container: StartedPostgreSqlContainer,
  sql: string,
  opts?: {
    db?: string;
    user?: string;
    psqlPath?: string;
    sslmode?: 'disable' | 'prefer' | 'require' | 'verify-ca' | 'verify-full';
  },
): Promise<{ exitCode: number; stdout: string; stderr: string; args: string[] }> {
  const host = container.getHost();
  const port = container.getPort();
  const db = opts?.db ?? container.getDatabase();
  const user = opts?.user ?? container.getUsername();
  const sslmode = opts?.sslmode ?? 'disable'; // local PG images don’t use TLS

  const args = [
    '-h',
    host,
    '-p',
    String(port),
    '-U',
    user,
    '-d',
    db,
    '-v',
    'ON_ERROR_STOP=1',
    '-tA', // tuples-only, unaligned
    '-w', // never prompt for password
    '-c',
    sql,
  ];
  const env = { ...process.env, PGPASSWORD: container.getPassword(), PGSSLMODE: sslmode };

  const cmd = opts?.psqlPath ?? 'psql';

  return await new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { env });
    let stdout = '',
      stderr = '';
    child.stdout.on('data', (d) => (stdout += String(d)));
    child.stderr.on('data', (d) => (stderr += String(d)));
    child.on('error', reject);
    child.on('close', (code) => resolve({ exitCode: code ?? -1, stdout, stderr, args }));
  });
}

// Try direct within container
async function execPsqlInContainer(container: StartedPostgreSqlContainer, sql?: string) {
  const res1 = await container.exec([
    'psql',
    '-v',
    'ON_ERROR_STOP=1',
    '-U',
    container.getUsername(),
    '-d',
    container.getDatabase(),
    '-tA',
    '-w',
    '-c',
    sql ?? 'SELECT 1;',
  ]);
  return res1.stdout;
}

async function tryTCPConnection(container: StartedPostgreSqlContainer) {
  await new Promise<void>((resolve, reject) => {
    const s = net.createConnection({ host: container.getHost(), port: container.getPort() });
    s.once('connect', () => {
      s.end();
      resolve();
    });
    s.once('error', reject);
  });
}

const mkConn = (uri: string, db: string) => uri.replace(/\/[^/]*$/, `/${db}`);

describe('PG16 drop/create e2e', () => {
  let testShouldRun = false;

  beforeAll(async () => {
    if (!process.env.RUN_PGDATABASE_ADMIN_TEST) return;

    let container: StartedPostgreSqlContainer | undefined = undefined;
    try {
      container = await new PostgreSqlContainer('postgres:16')
        .withUsername('dbos')
        .withPassword('dbos')
        .withDatabase('foobar')
        .start();

      // Enable test container logs for debugging
      if (false) {
        const stream = await container!.logs();
        stream
          .on('data', (line: string) => console.log('[pg]', line.toString()))
          .on('err', (line: string) => console.error('[pg-err]', line.toString()));
      }

      // Check that the container is configured and can execute to the DB
      const s1 = await execPsqlInContainer(container);
      if (s1.trim() !== '1') throw new Error(`Unable to 'SELECT 1' in container; got ${s1}`);

      // Try plain TCP to make sure networking works
      await tryTCPConnection(container);

      // Try psql from host
      const { exitCode, stdout, stderr, args } = await execHostPsql(container, 'SELECT 1');
      if (exitCode !== 0 || stdout.trim() !== '1')
        throw new Error(`Could not use psql to reach container: ${stderr}, ${JSON.stringify(args)}`);

      // Try a PG connection
      const adminClient = new Client({
        connectionString: container.getConnectionUri(),
      });
      try {
        await adminClient.connect();
        await adminClient.query('SELECT 1;');
      } finally {
        try {
          await adminClient.end();
        } catch (e2) {}
      }

      testShouldRun = true;
    } catch (e) {
      console.error(`Unable to run PG container; skipping tests: ${e as Error}`);
    }

    if (container) {
      await container.stop();
    }
  }, 120_000);

  afterAll(async () => {}, 120_000);

  // TODO Tests:
  //  URL masking
  //  Errors for not connecting
  //  Errors from PG itself
  //  Admin DB not available
  //  Weird db names
  //  Replace mkConn?

  test('using url plus database', async () => {
    if (!testShouldRun) return;

    const container = await new PostgreSqlContainer('postgres:16')
      .withUsername('dbos')
      .withPassword('dbos')
      .withDatabase('notpostgres')
      .start();

    try {
      // Creation via the admin URL
      const dbName = 'idle_db1';
      expect((await ensurePGDatabase({ adminUrl: container.getConnectionUri(), dbToEnsure: dbName })).status).toBe(
        'created',
      );
      expect((await ensurePGDatabase({ adminUrl: container.getConnectionUri(), dbToEnsure: dbName })).status).toBe(
        'already_exists',
      );

      // Drop via the admin URL
      expect(
        (await dropPGDatabase({ adminUrl: container.getConnectionUri(), dbToDrop: dbName, logger: () => {} })).status,
      ).toBe('dropped');
      expect(
        (await dropPGDatabase({ adminUrl: container.getConnectionUri(), dbToDrop: dbName, logger: () => {} })).status,
      ).toBe('did_not_exist');
    } finally {
      await container.stop();
    }
  }, 120_000);

  test('using url with postgres fallback', async () => {
    if (!testShouldRun) return;

    const container = await new PostgreSqlContainer('postgres:16')
      .withUsername('dbos')
      .withPassword('dbos')
      .withDatabase('postgres')
      .start();

    try {
      // Creation via the target URL (falls back on 'postgres')
      const dbName = 'idle_db2';
      const target = mkConn(container.getConnectionUri(), dbName);
      expect((await ensurePGDatabase({ urlToEnsure: target })).status).toBe('created');
      expect((await ensurePGDatabase({ urlToEnsure: target })).status).toBe('already_exists');

      // Drop via the URL (falls back on 'postgres')
      expect((await dropPGDatabase({ urlToDrop: target, logger: () => {} })).status).toBe('dropped');
      expect((await dropPGDatabase({ urlToDrop: target, logger: () => {} })).status).toBe('did_not_exist');
    } finally {
      await container.stop();
    }
  }, 120_000);

  test('drop db negative', async () => {
    if (!testShouldRun) return;

    const container = await new PostgreSqlContainer('postgres:16')
      .withUsername('dbos')
      .withPassword('dbos')
      .withDatabase('foobar')
      .start();

    try {
      const target = mkConn(container.getConnectionUri(), 'never_existed');

      // This version of it is using a bogus URL, and the 'postgres' service database doesn't exist
      const res1 = await dropPGDatabase({ urlToDrop: target, logger: () => {} });
      expect(res1.status).toBe('did_not_exist');

      // This version of it is using a valid admin URL, and the 'postgres' service database doesn't exist
      const res2 = await dropPGDatabase({
        dbToDrop: 'never_existed',
        adminUrl: container.getConnectionUri(),
        logger: () => {},
      });
      expect(res2.status).toBe('did_not_exist');
    } finally {
      await container.stop();
    }
  }, 120_000);

  test('drop a busy DB (new PG uses WITH (FORCE))', async () => {
    if (!testShouldRun) return;

    const container = await new PostgreSqlContainer('postgres:16')
      .withUsername('dbos')
      .withPassword('dbos')
      .withDatabase('foobar')
      .start();

    try {
      const dbName = 'busy_db';
      expect((await ensurePGDatabase({ adminUrl: container.getConnectionUri(), dbToEnsure: dbName })).status).toBe(
        'created',
      );

      // open a blocker connection
      const busy = new Client({ connectionString: mkConn(container.getConnectionUri(), dbName) });
      await busy.connect();
      busy.on('error', (err: { code?: string; message?: string }) => {
        if (err?.code === '57P01' || /terminating connection/i.test(err?.message ?? '')) return;
        if (err?.code === '08006' || err?.code === '08003') return;
        console.error('busy client unexpected error:', err);
      });
      await busy.query('BEGIN'); // keep a transaction open

      const res = await dropPGDatabase({
        urlToDrop: mkConn(container.getConnectionUri(), dbName),
        logger: () => {},
      });

      // the dropper should terminate our backend and succeed
      expect(res.status).toBe('dropped');

      // cleanup if still connected (should be terminated by drop)
      try {
        await busy.end();
      } catch {}

      const adminClient = new Client({ connectionString: container.getConnectionUri() });
      await adminClient.connect();
      const check = await adminClient.query<{ one: number }>('SELECT 1 as one FROM pg_database WHERE datname=$1', [
        dbName,
      ]);
      expect(check.rowCount).toBe(0);
      try {
        await adminClient.end();
      } catch {}
    } finally {
      await container.stop();
    }
  }, 120_000);

  /*

  test('helpful failure when admin connect not possible', async () => {
    // Create a non-superuser owner + DB
    await adminClient.query(`CREATE ROLE appuser LOGIN PASSWORD 's3cret'`);
    const dbName = 'blocked_admin';
    await adminClient.query(`CREATE DATABASE "${dbName}" OWNER appuser`);

    // revoke CONNECT to /postgres for appuser
    await adminClient.query(`REVOKE CONNECT ON DATABASE postgres FROM PUBLIC`);
    await adminClient.query(`REVOKE CONNECT ON DATABASE postgres FROM appuser`);

    // craft target URL as appuser@/blocked_admin (no adminUrl provided)
    const u = new URL(adminUri);
    u.username = 'appuser';
    u.password = 's3cret';
    const target = mkConn(u.toString(), dbName);

    const res = await dropPGDatabase({
      urlToDrop: target,
      logger: () => {},
    });

    expect(res.status).toBe('failed');
    //expect(res.hint || '').toMatch(/Provide an admin\/alternate DB URL/i);

    // cleanup with superuser
    await adminClient.query(`GRANT CONNECT ON DATABASE postgres TO PUBLIC`);
    await adminClient.query(`DROP DATABASE IF EXISTS "${dbName}"`);
    await adminClient.query(`DROP ROLE IF EXISTS appuser`);
  }, 120_000);
  */
});
