import { spawnSync } from 'child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';
import { Client } from 'pg';
import { migrate, generateMigrationSQL, generateMigrationStatements } from '../src/cli/migrate';
import { allMigrations } from '../src/sysdb_migrations/internal/migrations';
import { runSysMigrationsPg } from '../src/sysdb_migrations/migration_runner';
import { ensureSystemDatabase } from '../src/system_database';
import { GlobalLogger } from '../src/telemetry/logs';
import { generateDBOSTestConfig } from './helpers';

const FUNNY_SCHEMA = 'F8nny_sCHem@-n@m3';
const LATEST_VERSION = allMigrations().length;
const UNREACHABLE_URL = 'postgres://nobody:nopass@nonexistent-host.invalid:1/no_such_db';

/** Print the migration SQL in-process, capturing stdout. */
async function printMigrationSQL(systemDatabaseUrl: string, schemaName?: string): Promise<string> {
  let out = '';
  const stdoutSpy = jest
    .spyOn(process.stdout, 'write')
    .mockImplementation((chunk: unknown, encodingOrCb?: unknown, cb?: unknown) => {
      out += String(chunk);
      const callback = typeof encodingOrCb === 'function' ? encodingOrCb : cb;
      if (typeof callback === 'function') (callback as () => void)();
      return true;
    });
  try {
    const status = await migrate([], systemDatabaseUrl, new GlobalLogger(), true, schemaName);
    expect(status).toBe(0);
  } finally {
    stdoutSpy.mockRestore();
  }
  return out;
}

describe('migrate --print-only', () => {
  test('generateMigrationSQL emits complete, terminated SQL with version bookkeeping', () => {
    const sql = generateMigrationSQL();
    expect(sql).toContain('-- For FRESH databases only');
    expect(sql).toContain('CREATE SCHEMA IF NOT EXISTS "dbos";');
    expect(sql).toContain(
      'create table "dbos"."dbos_migrations" ("version" bigint not null, constraint "dbos_migrations_pkey" primary key ("version"));',
    );
    expect(sql).toContain('create table "dbos"."operation_outputs"');
    expect(sql).toContain('create table "dbos"."workflow_status"');
    const version = allMigrations().length;
    expect(sql.trimEnd().endsWith(`INSERT INTO "dbos"."dbos_migrations" ("version") VALUES (${version});`)).toBe(true);
    // Only SQL and -- comments; every statement is terminated with a semicolon.
    for (const line of sql.split('\n')) {
      expect(line).not.toMatch(/\[(info|warn|error)\]/);
    }
  });

  test('fail-fast guard follows the dbos_migrations table creation', () => {
    const sql = generateMigrationSQL();
    const createIdx = sql.indexOf('create table "dbos"."dbos_migrations"');
    const guardIdx = sql.indexOf('this script is for fresh databases only. Use dbos migrate instead.');
    const nextMigrationIdx = sql.indexOf('-- migration 3');
    expect(createIdx).toBeGreaterThan(-1);
    expect(guardIdx).toBeGreaterThan(createIdx);
    expect(guardIdx).toBeLessThan(nextMigrationIdx);
    expect(sql).toContain('RAISE EXCEPTION');
    expect(sql).toContain(`SELECT "version" INTO current_version FROM "dbos"."dbos_migrations"`);
  });

  test('generateMigrationSQL double-quotes identifiers for a schema with special characters', () => {
    const sql = generateMigrationSQL(FUNNY_SCHEMA);
    expect(sql).toContain(`CREATE SCHEMA IF NOT EXISTS "${FUNNY_SCHEMA}";`);
    expect(sql).toContain(`create table "${FUNNY_SCHEMA}"."dbos_migrations"`);
    expect(sql).toContain(`create table "${FUNNY_SCHEMA}"."workflow_status"`);
    expect(sql).toContain(`DBOS schema "${FUNNY_SCHEMA}" is already at version %`);
    const version = allMigrations().length;
    expect(
      sql.trimEnd().endsWith(`INSERT INTO "${FUNNY_SCHEMA}"."dbos_migrations" ("version") VALUES (${version});`),
    ).toBe(true);
    expect(sql).not.toContain('"dbos".');
    // The schema name never appears unquoted before a dot (which psql would fold or reject).
    expect(sql).not.toMatch(new RegExp(`[^"]F8nny_sCHem@-n@m3"?\\.`));
  });

  test('generateMigrationSQL substitutes the schema name', () => {
    const sql = generateMigrationSQL('custom_schema');
    expect(sql).toContain('CREATE SCHEMA IF NOT EXISTS "custom_schema";');
    expect(sql).not.toContain('"dbos".');
  });

  test('delta script upgrades from version N with a version guard and no fresh prelude', () => {
    const fromVersion = 20;
    const sql = generateMigrationSQL('dbos', fromVersion);
    expect(sql).toContain(`-- DBOS system database migration SQL (delta: version ${fromVersion} -> ${LATEST_VERSION})`);
    expect(sql).toContain(`IF current_version IS DISTINCT FROM ${fromVersion} THEN`);
    expect(sql).toContain(`this delta script requires version ${fromVersion}`);
    // No fresh-database prelude or guard.
    expect(sql).not.toContain('CREATE SCHEMA');
    expect(sql).not.toContain('-- migration 1:');
    expect(sql).not.toContain(`-- migration ${fromVersion}:`);
    expect(sql).not.toContain('fresh databases only');
    expect(sql).not.toContain('INSERT INTO "dbos"."dbos_migrations"');
    // Applies exactly N+1..latest and records the version with an UPDATE.
    expect(sql).toContain(`-- migration ${fromVersion + 1}:`);
    expect(sql.trimEnd().endsWith(`UPDATE "dbos"."dbos_migrations" SET "version" = ${LATEST_VERSION};`)).toBe(true);
    // The guard is the first statement, before any migration.
    expect(sql.indexOf('IS DISTINCT FROM')).toBeLessThan(sql.indexOf(`-- migration ${fromVersion + 1}:`));
  });

  test('at the latest version, prints only a nothing-to-do comment', () => {
    expect(generateMigrationSQL('dbos', LATEST_VERSION)).toBe(
      `-- Database is already at the latest DBOS schema version (${LATEST_VERSION}); nothing to do.`,
    );
    expect(generateMigrationStatements('dbos', LATEST_VERSION)).toEqual([]);
  });

  test('migrate with printOnly on an unreachable database silently prints the full fresh script', async () => {
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    try {
      const out = await printMigrationSQL(UNREACHABLE_URL);
      expect(out).toBe(generateMigrationSQL() + '\n');
      expect(warnSpy).not.toHaveBeenCalled();
      expect(errorSpy).not.toHaveBeenCalled();
      expect(logSpy).not.toHaveBeenCalled();
    } finally {
      warnSpy.mockRestore();
      errorSpy.mockRestore();
      logSpy.mockRestore();
    }
  });

  test('spawned CLI with unreachable database prints pure SQL to stdout, nothing to stderr, exit 0', () => {
    const cliPath = path.resolve(__dirname, '..', 'dist', 'src', 'cli', 'cli.js');
    const workDir = mkdtempSync(path.join(tmpdir(), 'dbos-migrate-print-'));
    try {
      writeFileSync(
        path.join(workDir, 'dbos-config.yaml'),
        `name: migrateprinttest\nsystem_database_url: ${UNREACHABLE_URL}\n`,
      );
      const res = spawnSync(process.execPath, [cliPath, 'migrate', '--print-only'], {
        cwd: workDir,
        encoding: 'utf-8',
      });
      expect(res.status).toBe(0);
      expect(res.stderr).toBe('');
      expect(res.stdout).toBe(generateMigrationSQL() + '\n');
    } finally {
      rmSync(workDir, { recursive: true, force: true });
    }
  }, 30000);

  test('printed SQL applies end-to-end to a fresh database with a special-character schema', async () => {
    const config = generateDBOSTestConfig();
    const baseUrl = new URL(config.systemDatabaseUrl!);
    baseUrl.pathname = '/postgres';
    const dbName = 'migrate_print_sql_test_db';
    const dbUrl = new URL(baseUrl.toString());
    dbUrl.pathname = `/${dbName}`;

    const adminClient = new Client({ connectionString: baseUrl.toString() });
    await adminClient.connect();
    await adminClient.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE);`);
    await adminClient.query(`CREATE DATABASE ${dbName};`);
    await adminClient.end();

    const client = new Client({ connectionString: dbUrl.toString() });
    await client.connect();
    try {
      // Connection OK but schema/table missing: the CLI prints the full fresh script.
      expect(await printMigrationSQL(dbUrl.toString(), FUNNY_SCHEMA)).toBe(generateMigrationSQL(FUNNY_SCHEMA) + '\n');

      // Apply each printed statement in autocommit, as psql would.
      const statements = generateMigrationStatements(FUNNY_SCHEMA);
      for (const stmt of statements) {
        await client.query(stmt);
      }

      const schemaExists = await client.query<{ exists: boolean }>(
        'SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = $1)',
        [FUNNY_SCHEMA],
      );
      expect(schemaExists.rows[0].exists).toBe(true);

      for (const table of ['dbos_migrations', 'workflow_status', 'operation_outputs', 'notifications', 'streams']) {
        const tableExists = await client.query<{ exists: boolean }>(
          'SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)',
          [FUNNY_SCHEMA, table],
        );
        expect(tableExists.rows[0].exists).toBe(true);
      }

      const versionRows = await client.query<{ version: string }>(
        `SELECT "version" FROM "${FUNNY_SCHEMA}"."dbos_migrations"`,
      );
      expect(versionRows.rows.length).toBe(1);
      expect(Number(versionRows.rows[0].version)).toBe(allMigrations().length);

      // Re-running the fail-fast guard on the now-migrated database aborts.
      const guard = statements.find((s) => s.includes('fresh databases only'))!;
      expect(guard).toBeDefined();
      await expect(client.query(guard)).rejects.toThrow(
        `DBOS schema "${FUNNY_SCHEMA}" is already at version ${allMigrations().length}; this script is for fresh databases only. Use dbos migrate instead.`,
      );
    } finally {
      await client.end();
      const cleanupClient = new Client({ connectionString: baseUrl.toString() });
      await cleanupClient.connect();
      await cleanupClient.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE);`);
      await cleanupClient.end();
    }
  }, 60000);

  test('delta script end-to-end: partial database, delta print, apply, latest, guard mismatch', async () => {
    const partialVersion = 20;
    const config = generateDBOSTestConfig();
    const baseUrl = new URL(config.systemDatabaseUrl!);
    baseUrl.pathname = '/postgres';
    const dbName = 'migrate_print_delta_test_db';
    const dbUrl = new URL(baseUrl.toString());
    dbUrl.pathname = `/${dbName}`;

    const adminClient = new Client({ connectionString: baseUrl.toString() });
    await adminClient.connect();
    await adminClient.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE);`);
    await adminClient.query(`CREATE DATABASE ${dbName};`);
    await adminClient.end();

    const client = new Client({ connectionString: dbUrl.toString() });
    await client.connect();
    try {
      // Build a genuinely partial database: run the real migration runner
      // through migration N only.
      await runSysMigrationsPg(client, allMigrations(FUNNY_SCHEMA).slice(0, partialVersion), FUNNY_SCHEMA, {
        onWarn: () => {},
      });
      const partialRows = await client.query<{ version: string }>(
        `SELECT "version" FROM "${FUNNY_SCHEMA}"."dbos_migrations"`,
      );
      expect(Number(partialRows.rows[0].version)).toBe(partialVersion);

      // The CLI connects, detects version N, and prints only the delta.
      const printed = await printMigrationSQL(dbUrl.toString(), FUNNY_SCHEMA);
      expect(printed).toBe(generateMigrationSQL(FUNNY_SCHEMA, partialVersion) + '\n');
      expect(printed).toContain(`(delta: version ${partialVersion} -> ${LATEST_VERSION})`);
      expect(printed).toContain(`IF current_version IS DISTINCT FROM ${partialVersion} THEN`);
      expect(printed).toContain(`-- migration ${partialVersion + 1}:`);
      expect(printed).not.toContain('CREATE SCHEMA');
      expect(printed).not.toContain(`-- migration ${partialVersion}:`);
      expect(printed).not.toContain('fresh databases only');
      expect(printed).toContain(`"${FUNNY_SCHEMA}"."dbos_migrations"`);

      // Apply the delta statement by statement in autocommit, as psql would.
      const deltaStatements = generateMigrationStatements(FUNNY_SCHEMA, partialVersion);
      for (const stmt of deltaStatements) {
        await client.query(stmt);
      }
      const versionRows = await client.query<{ version: string }>(
        `SELECT "version" FROM "${FUNNY_SCHEMA}"."dbos_migrations"`,
      );
      expect(versionRows.rows.length).toBe(1);
      expect(Number(versionRows.rows[0].version)).toBe(LATEST_VERSION);

      // A real migration pass treats the delta-migrated database as up-to-date.
      await ensureSystemDatabase(dbUrl.toString(), new GlobalLogger(), undefined, FUNNY_SCHEMA);
      const afterEnsure = await client.query<{ version: string }>(
        `SELECT "version" FROM "${FUNNY_SCHEMA}"."dbos_migrations"`,
      );
      expect(afterEnsure.rows.length).toBe(1);
      expect(Number(afterEnsure.rows[0].version)).toBe(LATEST_VERSION);

      // At the latest version the CLI prints only the nothing-to-do comment.
      expect(await printMigrationSQL(dbUrl.toString(), FUNNY_SCHEMA)).toBe(
        `-- Database is already at the latest DBOS schema version (${LATEST_VERSION}); nothing to do.\n`,
      );

      // Guard mismatch: the version-N delta guard aborts on a database at a different version.
      const guard = deltaStatements[0];
      expect(guard).toContain('IS DISTINCT FROM');
      await expect(client.query(guard)).rejects.toThrow(
        `DBOS schema "${FUNNY_SCHEMA}" is at version ${LATEST_VERSION} but this delta script requires version ${partialVersion}. Regenerate it with dbos migrate --print-only.`,
      );
    } finally {
      await client.end();
      const cleanupClient = new Client({ connectionString: baseUrl.toString() });
      await cleanupClient.connect();
      await cleanupClient.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE);`);
      await cleanupClient.end();
    }
  }, 60000);
});
