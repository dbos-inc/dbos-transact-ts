import { migrate, generateMigrationSQL } from '../src/cli/migrate';
import { allMigrations } from '../src/sysdb_migrations/internal/migrations';
import { GlobalLogger } from '../src/telemetry/logs';

describe('migrate --print-only', () => {
  test('generateMigrationSQL emits complete, terminated SQL with version bookkeeping', () => {
    const sql = generateMigrationSQL();
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
});
