import { execSync, SpawnSyncReturns } from 'child_process';
import { GlobalLogger } from '../telemetry/logs';
import { ensureSystemDatabase } from '../system_database';
import { ensureDbosTables } from '../user_database';
import { ensurePGDatabase, maskDatabaseUrl } from '../datasource';

export async function migrate(
  migrationCommands: string[],
  databaseUrl: string,
  systemDatabaseUrl: string,
  logger: GlobalLogger,
) {
  const url = new URL(databaseUrl);
  const database = url.pathname.slice(1);

  let status = 0;

  if (databaseUrl) {
    logger.info(`Starting migration: creating database ${database} if it does not exist`);

    const res = await ensurePGDatabase({
      urlToEnsure: databaseUrl,
      logger: (msg: string) => logger.info(msg),
    });
    if (res.status === 'connection_error') {
      logger.warn(
        `Failed to check or create connection to app database ${maskDatabaseUrl(databaseUrl)}: ${res.hint ?? ''}\n  ${res.notes.join('\n')}`,
      );
    }
    if (res.status === 'failed') {
      logger.warn(
        `Application database does not exist and could not be created: ${maskDatabaseUrl(databaseUrl)}: ${res.hint ?? ''}\n  ${res.notes.join('\n')}`,
      );
    }
  }

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

  try {
    logger.info('Creating DBOS tables in user database.');
    if (databaseUrl) await ensureDbosTables(databaseUrl);
  } catch (e) {
    if (e instanceof Error) {
      logger.error(`Error installing DBOS table into user database: ${e.message}`);
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
