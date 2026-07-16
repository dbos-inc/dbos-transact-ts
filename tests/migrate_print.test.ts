import { Client } from 'pg';
import { migrate, generateMigrationSQL, generateMigrationStatements } from '../src/cli/migrate';
import { allMigrations } from '../src/sysdb_migrations/internal/migrations';
import { GlobalLogger } from '../src/telemetry/logs';
import { generateDBOSTestConfig } from './helpers';

const FUNNY_SCHEMA = 'F8nny_sCHem@-n@m3';

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

  test('migrate with printOnly writes SQL to stdout and executes nothing', async () => {
    let out = '';
    const stdoutSpy = jest.spyOn(process.stdout, 'write').mockImplementation((chunk) => {
      out += String(chunk);
      return true;
    });
    const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      const status = await migrate(
        ['echo should-not-run'],
        'postgres://nonexistent-host:1/no_such_db',
        new GlobalLogger(),
        true,
      );
      expect(status).toBe(0);
      expect(out).toBe(generateMigrationSQL() + '\n');
      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('--print-only'));
    } finally {
      stdoutSpy.mockRestore();
      warnSpy.mockRestore();
    }
  });

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
});
