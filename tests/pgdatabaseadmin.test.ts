import { Client } from 'pg';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import net from 'net';

import { deriveDatabaseUrl, dropPGDatabase, ensurePGDatabase, maskDatabaseUrl } from '../src/datasource';
import { spawn } from 'child_process';
import { DBOS } from '../src';

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

function makePGConnStr(
  username: string,
  password: string,
  host: string,
  port: string | number,
  database: string,
  timeout: number,
) {
  return `postgresql://${username}:${password}@${host}:${port}/${database}?connect_timeout=${timeout}`;
}

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

  test('url masking', () => {
    expect(maskDatabaseUrl('postgres://postgres:secret@localhost:5432/dbostest?connect_timeout=7')).toBe(
      'postgres://postgres:s****t@localhost:5432/dbostest?connect_timeout=7',
    );
  });

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
      expect(
        (await ensurePGDatabase({ adminUrl: container.getConnectionUri(), dbToEnsure: dbName, logger: () => {} }))
          .status,
      ).toBe('created');
      expect(
        (await ensurePGDatabase({ adminUrl: container.getConnectionUri(), dbToEnsure: dbName, logger: () => {} }))
          .status,
      ).toBe('already_exists');

      // Drop via the admin URL
      expect(
        (await dropPGDatabase({ adminUrl: container.getConnectionUri(), dbToDrop: dbName, logger: () => {} })).status,
      ).toBe('dropped');
      expect(
        (await dropPGDatabase({ adminUrl: container.getConnectionUri(), dbToDrop: dbName, logger: () => {} })).status,
      ).toBe('did_not_exist');

      const dbNameYuck = `do"not;name-your$db.this!'`;
      expect(
        (await ensurePGDatabase({ adminUrl: container.getConnectionUri(), dbToEnsure: dbNameYuck, logger: () => {} }))
          .status,
      ).toBe('created');
      expect(
        (await dropPGDatabase({ adminUrl: container.getConnectionUri(), dbToDrop: dbNameYuck, logger: () => {} }))
          .status,
      ).toBe('dropped');
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
      const target = deriveDatabaseUrl(container.getConnectionUri(), dbName);
      expect((await ensurePGDatabase({ urlToEnsure: target, logger: () => {} })).status).toBe('created');
      expect((await ensurePGDatabase({ urlToEnsure: target, logger: () => {} })).status).toBe('already_exists');

      // Drop via the URL (falls back on 'postgres')
      expect((await dropPGDatabase({ urlToDrop: target, logger: () => {} })).status).toBe('dropped');
      expect((await dropPGDatabase({ urlToDrop: target, logger: () => {} })).status).toBe('did_not_exist');
    } finally {
      await container.stop();
    }
  }, 120_000);

  test('ensure drop db negative', async () => {
    if (!testShouldRun) return;

    const container = await new PostgreSqlContainer('postgres:16')
      .withUsername('dbos')
      .withPassword('dbos')
      .withDatabase('foobar')
      .start();

    try {
      // This version of it is using a bogus URL but is somewhat valid
      const targetWithPerms = makePGConnStr(
        'myuser',
        'mypassword',
        container.getHost(),
        container.getPort(),
        'mydatabase',
        1000,
      );
      const target = deriveDatabaseUrl(targetWithPerms, 'never_existed');
      const res1 = await dropPGDatabase({ urlToDrop: target, logger: () => {} });
      if (res1.status === 'failed') {
        expect(res1.hint?.toLowerCase()?.includes('invalid password')).toBeTruthy();
        expect(res1.message.toLowerCase().includes('could not establish any admin connection')).toBeTruthy();
      } else {
        expect(res1.status).toBe('failed');
      }

      const res1c = await ensurePGDatabase({ urlToEnsure: target, logger: () => {} });
      if (res1c.status === 'failed') {
        expect(res1c.hint?.toLowerCase()?.includes('invalid password')).toBeTruthy();
        expect(res1c.message.toLowerCase().includes('could not establish any admin connection')).toBeTruthy();
      } else {
        expect(res1c.status).toBe('failed');
      }

      // Same, but with incorrect admin + db name
      const res2 = await dropPGDatabase({ dbToDrop: 'never_existed', adminUrl: targetWithPerms, logger: () => {} });
      expect(res2.status).toBe('failed');
      if (res2.status === 'failed') {
        expect(res2.hint?.toLowerCase()?.includes('invalid password')).toBeTruthy();
        expect(res2.message.toLowerCase().includes('could not establish any admin connection')).toBeTruthy();
      } else {
        expect(res2.status).toBe('failed');
      }
      const res2c = await ensurePGDatabase({
        dbToEnsure: 'never_existed',
        adminUrl: targetWithPerms,
        logger: () => {},
      });
      expect(res2c.status).toBe('failed');
      if (res2c.status === 'failed') {
        expect(res2c.hint?.toLowerCase()?.includes('invalid password')).toBeTruthy();
        expect(res2c.message.toLowerCase().includes('could not establish any admin connection')).toBeTruthy();
      } else {
        expect(res2c.status).toBe('failed');
      }

      // Same, but with incorrect admin + db name
      const bogusServer = makePGConnStr('myuser', 'mypassword', container.getHost(), 59999, 'mydatabase', 1000);
      const res3 = await dropPGDatabase({ urlToDrop: bogusServer, logger: () => {} });
      expect(res3.status).toBe('failed');
      expect(res3.message.includes('connect failed'));
      const res3c = await ensurePGDatabase({ urlToEnsure: bogusServer, logger: () => {} });
      expect(res3c.status).toBe('failed');
      expect(res3c.message.includes('connect failed'));

      // Same, but with incorrect admin + db name; target is not existent
      const bogusAdminServer = makePGConnStr('myuser', 'mypassword', container.getHost(), 59999, 'mydatabase', 1000);
      const res4 = await dropPGDatabase({
        urlToDrop: deriveDatabaseUrl(container.getConnectionUri(), 'never_existed'),
        adminUrl: bogusAdminServer,
        logger: () => {},
      });
      expect(res4.status).toBe('did_not_exist');

      // Bogus admin
      const res4c = await ensurePGDatabase({
        urlToEnsure: container.getConnectionUri(),
        adminUrl: bogusAdminServer,
        logger: () => {},
      });
      expect(res4c.status).toBe('already_exists');
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
      expect(
        (await ensurePGDatabase({ adminUrl: container.getConnectionUri(), dbToEnsure: dbName, logger: () => {} }))
          .status,
      ).toBe('created');

      // open a blocker connection
      const busy = new Client({ connectionString: deriveDatabaseUrl(container.getConnectionUri(), dbName) });
      await busy.connect();
      busy.on('error', (err: { code?: string; message?: string }) => {
        if (err?.code === '57P01' || /terminat/i.test(err?.message ?? '')) return;
        if (err?.code === '08006' || err?.code === '08003') return;
        console.error('busy client unexpected error:', err);
      });
      await busy.query('BEGIN'); // keep a transaction open

      const res = await dropPGDatabase({
        urlToDrop: deriveDatabaseUrl(container.getConnectionUri(), dbName),
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

  test('helpful failure when admin connect not possible', async () => {
    if (!testShouldRun) return;

    const container = await new PostgreSqlContainer('postgres:16')
      .withUsername('dbos')
      .withPassword('dbos')
      .withDatabase('foobar')
      .start();

    try {
      const adminClient = new Client({ connectionString: container.getConnectionUri() });
      await adminClient.connect();

      // Create a non-superuser owner + DB
      await adminClient.query(`CREATE ROLE appuser LOGIN PASSWORD 's3cret'`);
      const noAdminDb = 'blocked_admin';
      await adminClient.query(`CREATE DATABASE "${noAdminDb}" OWNER appuser`);

      // revoke CONNECT to /postgres for appuser
      await adminClient.query(`REVOKE CONNECT ON DATABASE postgres FROM PUBLIC`);
      await adminClient.query(`REVOKE CONNECT ON DATABASE postgres FROM appuser`);

      // Try creating (2 ways to fail, OK if already exists)
      const userDb = makePGConnStr('appuser', 's3cret', container.getHost(), container.getPort(), noAdminDb, 1000);
      const createUserDb = deriveDatabaseUrl(userDb, 'cant_create');
      const res1c = await ensurePGDatabase({ urlToEnsure: createUserDb, logger: () => {} });
      expect(res1c.status).toBe('failed');
      expect(res1c.message.includes('permission denied')).toBeTruthy();
      expect(res1c.notes.find((s) => s.includes('permission denied for database "postgres"'))).toBeDefined(); // Tried connecting to 'postgres'

      const res3c = await ensurePGDatabase({ dbToEnsure: 'cant_create', adminUrl: userDb, logger: () => {} });
      expect(res3c.status).toBe('failed');
      expect(res3c.message.includes('permission denied')).toBeTruthy();
      expect(res3c.notes.find((s) => s.includes('permission denied for database "postgres"'))).toBeUndefined(); // admin given, do not try 'postgres'

      const res2c = await ensurePGDatabase({ urlToEnsure: userDb, logger: () => {} });
      expect(res2c.status).toBe('already_exists');

      // Try dropping (doesn't exist, vs cannot drop)
      const res1d = await dropPGDatabase({ urlToDrop: createUserDb, logger: () => {} });
      expect(res1d.status).toBe('did_not_exist');
      const res2d = await dropPGDatabase({ dbToDrop: 'cant_create', adminUrl: userDb, logger: () => {} });
      expect(res2d.status).toBe('did_not_exist');

      // Create it and find it cannot be dropped
      const dropUserDb = deriveDatabaseUrl(userDb, 'cant_drop');
      const resc = await ensurePGDatabase({
        dbToEnsure: 'cant_drop',
        adminUrl: container.getConnectionUri(),
        logger: () => {},
      });
      expect(resc.status).toBe('created');

      const res3d = await dropPGDatabase({ urlToDrop: dropUserDb, logger: () => {} });
      expect(res3d.status).toBe('failed');
      expect(res3d.message.includes('must be owner')).toBeTruthy();
      expect(res3d.notes.find((s) => s.includes('permission denied for database "postgres"'))).toBeDefined(); // Tried connecting to 'postgres'
      const res4d = await dropPGDatabase({ dbToDrop: 'cant_drop', adminUrl: userDb, logger: () => {} });
      expect(res4d.status).toBe('failed');
      expect(res4d.message.includes('must be owner')).toBeTruthy();
      expect(res4d.notes.find((s) => s.includes('permission denied for database "postgres"'))).toBeUndefined(); // admin given, do not try 'postgres'

      try {
        await adminClient.end();
      } catch {}
    } finally {
      await container.stop();
    }
  }, 120_000);

  test('launch DBOS with weird sysdb name', async () => {
    if (!testShouldRun) return;

    const container = await new PostgreSqlContainer('postgres:16')
      .withUsername('dbos')
      .withPassword('dbos')
      .withDatabase('foobar')
      .start();

    try {
      const dbName = `"db".'v1'-a$6`;
      expect(
        (await ensurePGDatabase({ adminUrl: container.getConnectionUri(), dbToEnsure: dbName, logger: () => {} }))
          .status,
      ).toBe('created');

      DBOS.setConfig({
        name: 'weird-sysdb-test',
        systemDatabaseUrl: makePGConnStr(
          container.getUsername(),
          container.getPassword(),
          container.getHost(),
          container.getPort(),
          dbName,
          3000,
        ),
      });

      const testWorkflow = DBOS.registerWorkflow(
        async (testValue: string) => {
          const step1 = await DBOS.runStep(async () => {
            return Promise.resolve('1');
          });
          const step2 = await DBOS.runStep(async () => {
            return Promise.resolve('2');
          });
          return testValue + step1 + step2;
        },
        { name: 'weird-sysdb-test-workflow' },
      );

      await DBOS.launch();
      try {
        const res = await testWorkflow('a');
        expect(res).toBe('a12');
      } finally {
        await DBOS.shutdown();
      }
      const dres = await dropPGDatabase({ adminUrl: container.getConnectionUri(), dbToDrop: dbName, logger: () => {} });
      expect(dres.status).toBe('dropped');
    } finally {
      await container.stop();
    }
  }, 120_000);
});
