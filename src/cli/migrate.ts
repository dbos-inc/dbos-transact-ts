import { execSync, SpawnSyncReturns } from 'child_process';
import { GlobalLogger } from '../telemetry/logs';
import { ensureSystemDatabase } from '../system_database';
import { allMigrations } from '../sysdb_migrations/internal/migrations';

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

type PrintedSection = { comment: string; statements: string[] };

function printedSections(schemaName: string): PrintedSection[] {
  const migrations = allMigrations(schemaName);
  const sections: PrintedSection[] = [];
  for (let i = 0; i < migrations.length; i++) {
    const m = migrations[i];
    const stmts = (m.pg ?? []).map((s) => {
      const stmt = s.trim();
      return stmt.endsWith(';') ? stmt : `${stmt};`;
    });
    if (stmts.length === 0) continue;
    sections.push({ comment: `-- migration ${i + 1}${m.name ? `: ${m.name}` : ''}`, statements: stmts });
    if (stmts.some((s) => /create table .*"dbos_migrations"/i.test(s))) {
      sections.push({
        comment: '-- abort if this database has already been migrated (fresh databases only)',
        statements: [freshDatabaseGuard(schemaName)],
      });
    }
  }
  sections.push({
    comment: '-- record applied migration version',
    statements: [`INSERT INTO "${schemaName}"."dbos_migrations" ("version") VALUES (${migrations.length});`],
  });
  return sections;
}

/** Individual executable statements, in order, for tests and tooling. */
export function generateMigrationStatements(schemaName: string = 'dbos'): string[] {
  return printedSections(schemaName).flatMap((s) => s.statements);
}

export function generateMigrationSQL(schemaName: string = 'dbos'): string {
  const lines: string[] = [
    '-- DBOS system database migration SQL',
    '-- For FRESH databases only: the script aborts if the dbos_migrations table',
    '-- already contains a row. Use `dbos migrate` for existing databases.',
    '-- Run with psql in autocommit mode (the default); some statements use',
    '-- CREATE INDEX CONCURRENTLY and cannot run inside a transaction block.',
  ];
  for (const section of printedSections(schemaName)) {
    lines.push('', section.comment, ...section.statements);
  }
  return lines.join('\n');
}

export async function migrate(
  migrationCommands: string[],
  systemDatabaseUrl: string,
  logger: GlobalLogger,
  printOnly: boolean = false,
  schemaName: string = 'dbos',
) {
  if (printOnly) {
    if (migrationCommands.length > 0) {
      console.warn(
        'Skipping user-defined migration commands from dbos-config.yaml in --print-only mode (they are shell commands, not SQL).',
      );
    }
    process.stdout.write(generateMigrationSQL(schemaName) + '\n');
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
