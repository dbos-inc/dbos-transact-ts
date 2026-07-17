import { execSync, SpawnSyncReturns } from 'child_process';
import { Client } from 'pg';
import { GlobalLogger } from '../telemetry/logs';
import { ensureSystemDatabase } from '../system_database';
import { allMigrations } from '../sysdb_migrations/internal/migrations';
import { getCurrentSysDBVersion } from '../sysdb_migrations/migration_runner';

function freshDatabaseGuard(schemaName: string): string {
  return `DO $$
DECLARE
  current_version bigint;
BEGIN
  SELECT "version" INTO current_version FROM "${schemaName}"."dbos_migrations" ORDER BY "version" DESC LIMIT 1;
  IF current_version IS NOT NULL THEN
    RAISE EXCEPTION 'DBOS schema "${schemaName}" is already at version %; this script is for fresh databases only. Use dbos migrate instead.', current_version;
  END IF;
END
$$;`;
}

function deltaVersionGuard(schemaName: string, fromVersion: number): string {
  return `DO $$
DECLARE
  current_version bigint;
BEGIN
  SELECT "version" INTO current_version FROM "${schemaName}"."dbos_migrations" ORDER BY "version" DESC LIMIT 1;
  IF current_version IS DISTINCT FROM ${fromVersion} THEN
    RAISE EXCEPTION 'DBOS schema "${schemaName}" is at version % but this delta script requires version ${fromVersion}. Regenerate it with dbos migrate --print-only.', current_version;
  END IF;
END
$$;`;
}

type PrintedSection = { comment: string; statements: string[] };

function printedSections(schemaName: string, fromVersion: number): PrintedSection[] {
  const migrations = allMigrations(schemaName);
  const sections: PrintedSection[] = [];
  if (fromVersion > 0) {
    sections.push({
      comment: `-- abort unless the database is exactly at DBOS schema version ${fromVersion}`,
      statements: [deltaVersionGuard(schemaName, fromVersion)],
    });
  }
  for (let i = fromVersion; i < migrations.length; i++) {
    const m = migrations[i];
    const stmts = (m.pg ?? []).map((s) => {
      const stmt = s.trim();
      return stmt.endsWith(';') ? stmt : `${stmt};`;
    });
    if (stmts.length === 0) continue;
    sections.push({ comment: `-- migration ${i + 1}${m.name ? `: ${m.name}` : ''}`, statements: stmts });
    if (fromVersion === 0 && stmts.some((s) => /create table .*"dbos_migrations"/i.test(s))) {
      sections.push({
        comment: '-- abort if this database has already been migrated (fresh databases only)',
        statements: [freshDatabaseGuard(schemaName)],
      });
    }
  }
  sections.push({
    comment: '-- record applied migration version',
    statements: [
      fromVersion === 0
        ? `INSERT INTO "${schemaName}"."dbos_migrations" ("version") VALUES (${migrations.length});`
        : `UPDATE "${schemaName}"."dbos_migrations" SET "version" = ${migrations.length};`,
    ],
  });
  return sections;
}

/**
 * Individual executable statements, in order, for tests and tooling.
 * fromVersion 0 generates the full fresh-database script; N > 0 generates a
 * delta upgrading a database at version N. Empty if already at the latest version.
 */
export function generateMigrationStatements(schemaName: string = 'dbos', fromVersion: number = 0): string[] {
  if (fromVersion >= allMigrations(schemaName).length) return [];
  return printedSections(schemaName, fromVersion).flatMap((s) => s.statements);
}

export function generateMigrationSQL(schemaName: string = 'dbos', fromVersion: number = 0): string {
  const latest = allMigrations(schemaName).length;
  if (fromVersion >= latest) {
    return `-- Database is already at the latest DBOS schema version (${fromVersion}); nothing to do.`;
  }
  const lines =
    fromVersion === 0
      ? [
          '-- DBOS system database migration SQL',
          '-- For FRESH databases only: the script aborts if the dbos_migrations table',
          '-- already contains a row. Use `dbos migrate` for existing databases.',
          '-- Run with psql in autocommit mode (the default); some statements use',
          '-- CREATE INDEX CONCURRENTLY and cannot run inside a transaction block.',
        ]
      : [
          `-- DBOS system database migration SQL (delta: version ${fromVersion} -> ${latest})`,
          `-- Upgrades a database at DBOS schema version ${fromVersion}; aborts if the current`,
          '-- version differs.',
          '-- Run with psql in autocommit mode (the default); some statements use',
          '-- CREATE INDEX CONCURRENTLY and cannot run inside a transaction block.',
        ];
  for (const section of printedSections(schemaName, fromVersion)) {
    lines.push('', section.comment, ...section.statements);
  }
  return lines.join('\n');
}

/**
 * Best-effort read of the current DBOS schema version. Returns 0 (fresh) if the
 * database is unreachable or the schema/table is missing. Never writes and
 * never emits output, so --print-only stdout/stderr stay pure SQL.
 */
async function tryGetCurrentVersion(systemDatabaseUrl: string, schemaName: string): Promise<number> {
  const client = new Client({ connectionString: systemDatabaseUrl, connectionTimeoutMillis: 5000 });
  try {
    await client.connect();
    return await getCurrentSysDBVersion(client, schemaName);
  } catch {
    return 0;
  } finally {
    try {
      await client.end();
    } catch {
      // ignore
    }
  }
}

export async function migrate(
  migrationCommands: string[],
  systemDatabaseUrl: string,
  logger: GlobalLogger,
  printOnly: boolean = false,
  schemaName: string = 'dbos',
) {
  if (printOnly) {
    // User-defined migration commands from dbos-config.yaml are shell commands,
    // not SQL, so they are skipped here. Nothing is logged in this mode: stdout
    // must stay pure SQL/comments so it can be piped into a .sql file.
    const fromVersion = await tryGetCurrentVersion(systemDatabaseUrl, schemaName);
    // Wait for the write to flush: the CLI exits via process.exit(), which
    // would otherwise truncate piped output.
    await new Promise<void>((resolve, reject) => {
      process.stdout.write(generateMigrationSQL(schemaName, fromVersion) + '\n', (err) =>
        err ? reject(err) : resolve(),
      );
    });
    return 0;
  }

  let status = 0;

  try {
    migrationCommands?.forEach((cmd) => {
      logger.info(`Executing migration command: ${cmd}`);
      const migrateCommandOutput = execSync(cmd, { encoding: 'utf-8' });
      console.log(migrateCommandOutput.trimEnd());
    });
  } catch (e) {
    logMigrationError(e, logger, 'Error running migration');
    status = 1;
  }

  logger.info('Creating DBOS system database.');
  try {
    await ensureSystemDatabase(systemDatabaseUrl, logger, undefined, schemaName);
  } catch (e) {
    if (e instanceof Error) {
      logger.error(`Error creating DBOS system database: ${e.message}`);
    } else {
      logger.error(e);
    }
    status = 1;
  }

  if (status === 0) {
    logger.info('All migration successful!');
  }
  return status;
}

type ExecSyncError<T> = Error & SpawnSyncReturns<T>;
//Test to determine if e can be treated as an ExecSyncError.
function isExecSyncError(e: Error): e is ExecSyncError<string | Buffer> {
  if (
    //Safeguard against NaN. NaN type is number but NaN !== NaN
    'pid' in e &&
    typeof e.pid === 'number' &&
    e.pid === e.pid &&
    'stdout' in e &&
    (Buffer.isBuffer(e.stdout) || typeof e.stdout === 'string') &&
    'stderr' in e &&
    (Buffer.isBuffer(e.stderr) || typeof e.stderr === 'string')
  ) {
    return true;
  }
  return false;
}

function logMigrationError(e: unknown, logger: GlobalLogger, title: string) {
  logger.error(title);
  if (e instanceof Error && isExecSyncError(e)) {
    const stderr = e.stderr;
    if (e.stderr.length > 0) {
      logger.error(`Standard Error: ${stderr.toString().trim()}`);
    }
    const stdout = e.stdout;
    if (stdout.length > 0) {
      logger.error(`Standard Output: ${stdout.toString().trim()}`);
    }
    if (e.message) {
      logger.error(e.message);
    }
    if (e.error?.message) {
      logger.error(e.error?.message);
    }
  } else {
    logger.error(e);
  }
}
