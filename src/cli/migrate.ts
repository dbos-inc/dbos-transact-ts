import { execSync, SpawnSyncReturns } from 'child_process';
import { GlobalLogger } from '../telemetry/logs';
import { ensureSystemDatabase, getDbosSchemaPermissionsSql } from '../system_database';
import { allMigrations } from '../sysdb_migrations/internal/migrations';
import { maskDatabaseUrl } from '../database_utils';

export type MigrateOptions = {
  schemaName?: string;
  printMigrations?: string;
  printUserRole?: boolean;
  appRole?: string;
};

type PrintedSection = { comment?: string; statements: string[] };

const CREATES_MIGRATIONS_TABLE = /create table .*"dbos_migrations"/i;

function migrationSections(schemaName: string, startMigration: number): PrintedSection[] {
  const migrations = allMigrations(schemaName);
  const sections: PrintedSection[] = [];
  // A database at version V has run migrations 1..V, so the dbos_migrations
  // table (and its single version row) exists iff the migration creating it
  // is before startMigration.
  let tableExists = migrations
    .slice(0, startMigration - 1)
    .some((m) => (m.pg ?? []).some((s) => CREATES_MIGRATIONS_TABLE.test(s)));
  let versionRowExists = tableExists;
  for (let i = startMigration; i <= migrations.length; i++) {
    const m = migrations[i - 1];
    const stmts = (m.pg ?? [])
      .map((s) => s.trim())
      .filter((s) => s !== '')
      .map((s) => (s.endsWith(';') ? s : `${s};`));
    const statements = [...stmts];
    if (!tableExists) {
      tableExists = stmts.some((s) => CREATES_MIGRATIONS_TABLE.test(s));
    }
    // Per-migration version bookkeeping, mirroring the runner: an interrupted
    // apply can be resumed from the next migration number. The runner records
    // nothing until a migration has created the dbos_migrations table.
    if (tableExists) {
      if (versionRowExists) {
        statements.push(`UPDATE "${schemaName}"."dbos_migrations" SET "version" = ${i};`);
      } else {
        statements.push(`INSERT INTO "${schemaName}"."dbos_migrations" ("version") VALUES (${i});`);
        versionRowExists = true;
      }
    }
    if (statements.length === 0) continue;
    sections.push({
      comment: stmts.length > 0 ? `-- Migration ${i}${m.name ? `: ${m.name}` : ''}` : undefined,
      statements,
    });
  }
  return sections;
}

/**
 * Individual executable statements, in order, for migrations startMigration
 * (1-based) through the latest, including per-migration version bookkeeping.
 */
export function generateMigrationStatements(schemaName: string = 'dbos', startMigration: number = 1): string[] {
  return migrationSections(schemaName, startMigration).flatMap((s) => s.statements);
}

/**
 * The SQL script for migrations startMigration (1-based) through the latest:
 * pure SQL and -- comments, applicable with plain psql. Never touches a database.
 */
export function generateMigrationSQL(schemaName: string = 'dbos', startMigration: number = 1): string {
  const lines: string[] = [
    '-- This script is for PostgreSQL only.',
    '-- Contains CREATE/DROP INDEX CONCURRENTLY: run outside a transaction block (e.g. plain psql, not psql --single-transaction).',
  ];
  if (startMigration === 1) {
    lines.push('-- This script is for FRESH databases only.');
  }
  for (const section of migrationSections(schemaName, startMigration)) {
    if (section.comment) lines.push(section.comment);
    lines.push(...section.statements);
  }
  return lines.join('\n');
}

// Wait for the write to flush: the CLI exits via process.exit(), which would
// otherwise truncate piped output.
function writeAndFlush(stream: NodeJS.WriteStream, text: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    stream.write(text, (err) => (err ? reject(err) : resolve()));
  });
}

async function fail(message: string): Promise<number> {
  await writeAndFlush(process.stderr, message + '\n');
  return 1;
}

function quotedIdentifierError(name: string, kind: string): string | undefined {
  return name.includes('"') || name.includes("'") ? `${kind} names containing quotes are not supported` : undefined;
}

async function runPrintMode(systemDatabaseUrl: string, options: MigrateOptions): Promise<number> {
  const schemaName = options.schemaName ?? 'dbos';
  if (options.printMigrations !== undefined && options.printUserRole) {
    return await fail('--print-user-role cannot be combined with --print-migrations');
  }

  if (options.printUserRole) {
    if (!options.appRole) return await fail('--print-user-role requires --app-role');
    const error = quotedIdentifierError(schemaName, 'Schema') ?? quotedIdentifierError(options.appRole, 'Role');
    if (error) return await fail(error);
    const lines = [
      `-- Permissions on DBOS schema ${schemaName} for role ${options.appRole}`,
      ...getDbosSchemaPermissionsSql(schemaName, options.appRole).map((s) => `${s};`),
    ];
    await writeAndFlush(process.stdout, lines.join('\n') + '\n');
    return 0;
  }

  const value = options.printMigrations!;
  const error = quotedIdentifierError(schemaName, 'Schema');
  if (error) return await fail(error);
  const latest = allMigrations(schemaName).length;
  let start = 1;
  if (value !== 'all') {
    if (!/^-?\d+$/.test(value)) {
      return await fail(`Invalid --print-migrations value '${value}': expected 'all' or a migration number`);
    }
    start = Number(value);
    if (start < 1 || start > latest) {
      return await fail(`Migration ${start} does not exist: valid migrations are 1 through ${latest}`);
    }
  }
  const header = `-- DBOS system database migrations for ${maskDatabaseUrl(systemDatabaseUrl)}`;
  await writeAndFlush(process.stdout, `${header}\n${generateMigrationSQL(schemaName, start)}\n`);
  return 0;
}

export async function migrate(
  migrationCommands: string[],
  systemDatabaseUrl: string,
  logger: GlobalLogger,
  options: MigrateOptions = {},
) {
  const schemaName = options.schemaName ?? 'dbos';
  if (options.printMigrations !== undefined || options.printUserRole) {
    // User-defined migration commands from dbos-config.yaml are shell commands,
    // not SQL, so they are skipped here. Nothing is logged in print modes:
    // stdout must stay pure SQL/comments so it can be piped into a .sql file.
    return await runPrintMode(systemDatabaseUrl, options);
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
