import { execSync, SpawnSyncReturns } from 'child_process';
import { GlobalLogger } from '../telemetry/logs';
import { ensureSystemDatabase } from '../system_database';
import { allMigrations } from '../sysdb_migrations/internal/migrations';

export function generateMigrationSQL(schemaName: string = 'dbos'): string {
  const migrations = allMigrations(schemaName);
  const lines: string[] = [
    '-- DBOS system database migration SQL',
    '-- Run with psql in autocommit mode (the default); some statements use',
    '-- CREATE INDEX CONCURRENTLY and cannot run inside a transaction block.',
  ];
  for (let i = 0; i < migrations.length; i++) {
    const m = migrations[i];
    const stmts = m.pg ?? [];
    if (stmts.length === 0) continue;
    lines.push('', `-- migration ${i + 1}${m.name ? `: ${m.name}` : ''}`);
    for (const s of stmts) {
      const stmt = s.trim();
      lines.push(stmt.endsWith(';') ? stmt : `${stmt};`);
    }
  }
  lines.push('', '-- record applied migration version');
  lines.push(`INSERT INTO "${schemaName}"."dbos_migrations" ("version") VALUES (${migrations.length});`);
  return lines.join('\n');
}

export async function migrate(
  migrationCommands: string[],
  systemDatabaseUrl: string,
  logger: GlobalLogger,
  printOnly: boolean = false,
) {
  if (printOnly) {
    if (migrationCommands.length > 0) {
      console.warn(
        'Skipping user-defined migration commands from dbos-config.yaml in --print-only mode (they are shell commands, not SQL).',
      );
    }
    process.stdout.write(generateMigrationSQL() + '\n');
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
    await ensureSystemDatabase(systemDatabaseUrl, logger);
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
