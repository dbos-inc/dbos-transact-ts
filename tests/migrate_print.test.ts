import { spawnSync } from 'child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';
import { Client } from 'pg';
import { migrate, generateMigrationSQL, generateMigrationStatements, MigrateOptions } from '../src/cli/migrate';
import { allMigrations } from '../src/sysdb_migrations/internal/migrations';
import { getCurrentSysDBVersion } from '../src/sysdb_migrations/migration_runner';
import { ensureSystemDatabase } from '../src/system_database';
import { maskDatabaseUrl } from '../src/database_utils';
import { GlobalLogger } from '../src/telemetry/logs';
import { generateDBOSTestConfig } from './helpers';

const FUNNY_SCHEMA = 'F8nny_sCHem@-n@m3';
const LATEST = allMigrations().length;
const UNREACHABLE_URL = 'postgres://nobody:nopass@nonexistent-host.invalid:1/no_such_db';

type CliResult = { status: number; out: string; err: string };

/** Run migrate() in-process, capturing stdout and stderr. */
async function runMigrate(url: string, options: MigrateOptions): Promise<CliResult> {
  let out = '';
  let err = '';
  const impl = (sink: (s: string) => void) =>
    ((chunk: unknown, encodingOrCb?: unknown, cb?: unknown) => {
      sink(String(chunk));
      const callback = typeof encodingOrCb === 'function' ? encodingOrCb : cb;
      if (typeof callback === 'function') (callback as () => void)();
      return true;
    }) as typeof process.stdout.write;
  const outSpy = jest.spyOn(process.stdout, 'write').mockImplementation(impl((s) => (out += s)));
  const errSpy = jest.spyOn(process.stderr, 'write').mockImplementation(impl((s) => (err += s)));
  try {
    const status = await migrate([], url, new GlobalLogger(), options);
    return { status, out, err };
  } finally {
    outSpy.mockRestore();
    errSpy.mockRestore();
  }
}

function testUrls(dbName: string): { adminUrl: string; dbUrl: string } {
  const config = generateDBOSTestConfig();
  const adminUrl = new URL(config.systemDatabaseUrl!);
  adminUrl.pathname = '/postgres';
  const dbUrl = new URL(adminUrl.toString());
  dbUrl.pathname = `/${dbName}`;
  return { adminUrl: adminUrl.toString(), dbUrl: dbUrl.toString() };
}

async function recreateDatabase(adminUrl: string, dbName: string): Promise<void> {
  const client = new Client({ connectionString: adminUrl });
  await client.connect();
  try {
    await client.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE);`);
    await client.query(`CREATE DATABASE ${dbName};`);
  } finally {
    await client.end();
  }
}

async function dropDatabase(adminUrl: string, dbName: string): Promise<void> {
  const client = new Client({ connectionString: adminUrl });
  await client.connect();
  try {
    await client.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE);`);
  } finally {
    await client.end();
  }
}

describe('migrate --print-migrations and --print-user-role', () => {
  test('print-migrations all: headers, per-migration bookkeeping, pure SQL, no database needed', async () => {
    const { status, out, err } = await runMigrate(UNREACHABLE_URL, { printMigrations: 'all' });
    expect(status).toBe(0);
    expect(err).toBe('');

    const lines = out.split('\n');
    expect(lines[0]).toBe(`-- DBOS system database migrations for ${maskDatabaseUrl(UNREACHABLE_URL)}`);
    expect(lines[0]).not.toContain('nopass');
    expect(lines[1]).toBe(
      '-- Contains CREATE/DROP INDEX CONCURRENTLY: run outside a transaction block (e.g. plain psql, not psql --single-transaction).',
    );
    expect(lines[2]).toBe('-- This script is for FRESH databases only.');

    // The migrations themselves create the schema and the dbos_migrations table.
    expect(out).toContain('-- Migration 1: 20240123182943_schema');
    expect(out).toContain('CREATE SCHEMA IF NOT EXISTS "dbos";');
    expect(out).toContain('-- Migration 2: 20240123182944_dbos_migrations');

    // Per-migration bookkeeping mirrors the runner: nothing before the
    // dbos_migrations table exists, INSERT once, then UPDATE per migration.
    expect(out).not.toContain('("version") VALUES (1)');
    expect(out).toContain('INSERT INTO "dbos"."dbos_migrations" ("version") VALUES (2);');
    expect(out.match(/INSERT INTO "dbos"\."dbos_migrations"/g)).toHaveLength(1);
    for (let i = 3; i <= LATEST; i++) {
      expect(out).toContain(`UPDATE "dbos"."dbos_migrations" SET "version" = ${i};`);
    }
    expect(out.trimEnd().endsWith(`UPDATE "dbos"."dbos_migrations" SET "version" = ${LATEST};`)).toBe(true);

    // No guard blocks, no role grants, no log noise.
    expect(out).not.toContain('DO $$');
    expect(out).not.toContain('GRANT');
    for (const line of lines) {
      expect(line).not.toMatch(/\[(info|warn|error)\]/);
    }
  });

  test('print-migrations 1 is identical to all', async () => {
    const all = await runMigrate(UNREACHABLE_URL, { printMigrations: 'all' });
    const one = await runMigrate(UNREACHABLE_URL, { printMigrations: '1' });
    expect(one.status).toBe(0);
    expect(one.out).toBe(all.out);
    expect(generateMigrationSQL('dbos', 1)).toBe(generateMigrationSQL());
  });

  test('print-migrations N omits the prelude and earlier migrations', async () => {
    const { status, out, err } = await runMigrate(UNREACHABLE_URL, { printMigrations: '10' });
    expect(status).toBe(0);
    expect(err).toBe('');
    expect(out).not.toContain('CREATE SCHEMA');
    expect(out).not.toContain('FRESH databases');
    expect(out).not.toContain('-- Migration 9');
    expect(out).not.toContain('INSERT INTO "dbos"."dbos_migrations"');
    expect(out).toContain('-- Migration 10');
    expect(out).toContain('-- Migration 11');
    expect(out).toContain('UPDATE "dbos"."dbos_migrations" SET "version" = 10;');
    expect(out).toContain(`UPDATE "dbos"."dbos_migrations" SET "version" = ${LATEST};`);
    expect(out).not.toContain('DO $$');
  });

  test('invalid print-migrations values are rejected on stderr with nothing on stdout', async () => {
    for (const bad of ['0', String(LATEST + 1), '-1']) {
      const res = await runMigrate(UNREACHABLE_URL, { printMigrations: bad });
      expect(res.status).toBe(1);
      expect(res.out).toBe('');
      expect(res.err).toBe(`Migration ${bad} does not exist: valid migrations are 1 through ${LATEST}\n`);
    }
    const res = await runMigrate(UNREACHABLE_URL, { printMigrations: 'foo' });
    expect(res.status).toBe(1);
    expect(res.out).toBe('');
    expect(res.err).toBe(`Invalid --print-migrations value 'foo': expected 'all' or a migration number\n`);
  });

  test('schema and role names containing quotes are rejected', async () => {
    let res = await runMigrate(UNREACHABLE_URL, { printMigrations: 'all', schemaName: 'bad"schema' });
    expect(res.status).toBe(1);
    expect(res.out).toBe('');
    expect(res.err).toBe('Schema names containing quotes are not supported\n');

    res = await runMigrate(UNREACHABLE_URL, { printUserRole: true, appRole: "bad'role" });
    expect(res.status).toBe(1);
    expect(res.out).toBe('');
    expect(res.err).toBe('Role names containing quotes are not supported\n');
  });

  test('print-user-role requires app-role, excludes print-migrations, and emits only grant SQL', async () => {
    let res = await runMigrate(UNREACHABLE_URL, { printUserRole: true });
    expect(res.status).toBe(1);
    expect(res.out).toBe('');
    expect(res.err).toBe('--print-user-role requires --app-role\n');

    res = await runMigrate(UNREACHABLE_URL, { printMigrations: 'all', printUserRole: true, appRole: 'my-app-role' });
    expect(res.status).toBe(1);
    expect(res.out).toBe('');
    expect(res.err).toBe('--print-user-role cannot be combined with --print-migrations\n');

    res = await runMigrate(UNREACHABLE_URL, { printUserRole: true, appRole: 'my-app-role', schemaName: FUNNY_SCHEMA });
    expect(res.status).toBe(0);
    expect(res.err).toBe('');
    const lines = res.out.trimEnd().split('\n');
    expect(lines[0]).toBe(`-- Permissions on DBOS schema ${FUNNY_SCHEMA} for role my-app-role`);
    expect(lines).toHaveLength(8);
    expect(lines).toContain(`GRANT USAGE ON SCHEMA "${FUNNY_SCHEMA}" TO "my-app-role";`);
    expect(lines).toContain(
      `ALTER DEFAULT PRIVILEGES IN SCHEMA "${FUNNY_SCHEMA}" GRANT EXECUTE ON FUNCTIONS TO "my-app-role";`,
    );
    for (const line of lines) {
      expect(line.startsWith('--') || line.startsWith('GRANT') || line.startsWith('ALTER')).toBe(true);
    }
  });

  test('printed statements apply to a fresh database and the runner then treats it as migrated', async () => {
    const dbName = 'migrate_print_sql_test_db';
    const { adminUrl, dbUrl } = testUrls(dbName);
    await recreateDatabase(adminUrl, dbName);

    const { status, out, err } = await runMigrate(dbUrl, { printMigrations: 'all', schemaName: FUNNY_SCHEMA });
    expect(status).toBe(0);
    expect(err).toBe('');
    expect(out).toContain(`CREATE SCHEMA IF NOT EXISTS "${FUNNY_SCHEMA}";`);
    // The schema name never appears unquoted before a dot (psql would fold or reject it).
    expect(out).not.toMatch(/[^"]F8nny_sCHem@-n@m3"?\./);
    expect(out).not.toContain('"dbos".');

    const client = new Client({ connectionString: dbUrl });
    await client.connect();
    try {
      // Apply each printed statement in autocommit, as psql would.
      for (const stmt of generateMigrationStatements(FUNNY_SCHEMA)) {
        await client.query(stmt);
      }
      const versionRows = await client.query<{ version: string }>(
        `SELECT "version" FROM "${FUNNY_SCHEMA}"."dbos_migrations"`,
      );
      expect(versionRows.rows).toHaveLength(1);
      expect(Number(versionRows.rows[0].version)).toBe(LATEST);

      // A real migration pass treats the scripted database as fully migrated.
      await ensureSystemDatabase(dbUrl, new GlobalLogger(), undefined, FUNNY_SCHEMA);
      expect(await getCurrentSysDBVersion(client, FUNNY_SCHEMA)).toBe(LATEST);
      const afterEnsure = await client.query(`SELECT "version" FROM "${FUNNY_SCHEMA}"."dbos_migrations"`);
      expect(afterEnsure.rows).toHaveLength(1);
    } finally {
      await client.end();
      await dropDatabase(adminUrl, dbName);
    }
  }, 60000);

  test('a partial database is completed by the print-migrations latest output', async () => {
    const dbName = 'migrate_print_partial_test_db';
    const { adminUrl, dbUrl } = testUrls(dbName);
    await recreateDatabase(adminUrl, dbName);

    const client = new Client({ connectionString: dbUrl });
    await client.connect();
    try {
      // Truncate the full script right after the version latest-1 bookkeeping.
      const statements = generateMigrationStatements('dbos');
      const marker = `UPDATE "dbos"."dbos_migrations" SET "version" = ${LATEST - 1};`;
      const markerIdx = statements.indexOf(marker);
      expect(markerIdx).toBeGreaterThan(-1);
      for (const stmt of statements.slice(0, markerIdx + 1)) {
        await client.query(stmt);
      }
      expect(await getCurrentSysDBVersion(client, 'dbos')).toBe(LATEST - 1);

      // The last migration printed alone applies on top of version latest-1.
      const { status, out } = await runMigrate(dbUrl, { printMigrations: String(LATEST) });
      expect(status).toBe(0);
      expect(out).not.toContain('CREATE SCHEMA');
      expect(out).not.toContain('DO $$');
      for (const stmt of generateMigrationStatements('dbos', LATEST)) {
        await client.query(stmt);
      }
      expect(await getCurrentSysDBVersion(client, 'dbos')).toBe(LATEST);

      await ensureSystemDatabase(dbUrl, new GlobalLogger());
      expect(await getCurrentSysDBVersion(client, 'dbos')).toBe(LATEST);
    } finally {
      await client.end();
      await dropDatabase(adminUrl, dbName);
    }
  }, 60000);

  test('spawned CLI prints pure SQL to stdout, nothing to stderr, exit 0 with unreachable database', async () => {
    const cliPath = path.resolve(__dirname, '..', 'dist', 'src', 'cli', 'cli.js');
    const workDir = mkdtempSync(path.join(tmpdir(), 'dbos-migrate-print-'));
    try {
      writeFileSync(
        path.join(workDir, 'dbos-config.yaml'),
        `name: migrateprinttest\nsystem_database_url: ${UNREACHABLE_URL}\n`,
      );
      const spawn = (...args: string[]) =>
        spawnSync(process.execPath, [cliPath, 'migrate', ...args], { cwd: workDir, encoding: 'utf-8' });

      let res = spawn('--print-migrations', 'all');
      expect(res.status).toBe(0);
      expect(res.stderr).toBe('');
      expect(res.stdout).toBe((await runMigrate(UNREACHABLE_URL, { printMigrations: 'all' })).out);

      res = spawn('--print-user-role', '-r', 'my_app_role', '-s', 'custom_schema');
      expect(res.status).toBe(0);
      expect(res.stderr).toBe('');
      expect(res.stdout).toBe(
        (
          await runMigrate(UNREACHABLE_URL, {
            printUserRole: true,
            appRole: 'my_app_role',
            schemaName: 'custom_schema',
          })
        ).out,
      );

      res = spawn('--print-user-role');
      expect(res.status).toBe(1);
      expect(res.stdout).toBe('');
      expect(res.stderr).toContain('--app-role');

      res = spawn('--print-migrations', 'all', '--print-user-role', '-r', 'my_app_role');
      expect(res.status).toBe(1);
      expect(res.stdout).toBe('');
      expect(res.stderr).toContain('cannot be combined');

      res = spawn('--print-migrations', 'nope');
      expect(res.status).toBe(1);
      expect(res.stdout).toBe('');
      expect(res.stderr).toContain("expected 'all' or a migration number");
    } finally {
      rmSync(workDir, { recursive: true, force: true });
    }
  }, 60000);
});
