import { getDbosSchemaPermissionsSql } from '../system_database';
import { allMigrations } from '../sysdb_migrations/internal/migrations';
import { maskDatabaseUrl } from '../database_utils';

export type SchemaPrintOptions = {
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
  let lastEmitted = startMigration - 1;

  // Version bookkeeping mirroring the runner, which records nothing until a migration creates dbos_migrations.
  const versionStatement = (version: number): string | undefined => {
    if (!tableExists) return undefined;
    lastEmitted = version;
    if (versionRowExists) {
      return `UPDATE "${schemaName}"."dbos_migrations" SET "version" = ${version};`;
    }
    versionRowExists = true;
    return `INSERT INTO "${schemaName}"."dbos_migrations" ("version") VALUES (${version});`;
  };

  for (let i = startMigration; i <= migrations.length; i++) {
    const m = migrations[i - 1];
    const stmts = (m.pg ?? [])
      .map((s) => s.trim())
      .filter((s) => s !== '')
      .map((s) => (s.endsWith(';') ? s : `${s};`));
    // Renumbering onto the shared base leaves long runs of empty migrations; nothing to emit.
    if (stmts.length === 0) continue;
    if (!tableExists) {
      tableExists = stmts.some((s) => CREATES_MIGRATIONS_TABLE.test(s));
    }
    const statements = [...stmts];
    const version = versionStatement(i);
    if (version) statements.push(version);
    sections.push({
      comment: `-- Migration ${i}${m.name ? `: ${m.name}` : ''}`,
      statements,
    });
  }

  // Empty migrations at the end still count as applied.
  if (migrations.length > lastEmitted) {
    const version = versionStatement(migrations.length);
    if (version) sections.push({ statements: [version] });
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

/**
 * Print mode for `dbos schema`: writes SQL to stdout (or an error to stderr)
 * and returns the exit code. Never connects to a database, so a missing or
 * unreachable systemDatabaseUrl is not an error.
 */
export async function runSchemaPrintMode(
  systemDatabaseUrl: string | undefined,
  options: SchemaPrintOptions,
): Promise<number> {
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
  const header = `-- DBOS system database migrations${systemDatabaseUrl ? ` for ${maskDatabaseUrl(systemDatabaseUrl)}` : ''}`;
  await writeAndFlush(process.stdout, `${header}\n${generateMigrationSQL(schemaName, start)}\n`);
  return 0;
}
