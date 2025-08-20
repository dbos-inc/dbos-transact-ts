import { execSync, SpawnSyncReturns } from 'child_process';
import { GlobalLogger } from '../telemetry/logs';
import { ensureSystemDatabase } from '../system_database';
import { createDBIfDoesNotExist, ensureDbosTables } from '../user_database';
import { Client } from 'pg';
import { getClientConfig } from '../utils';

export async function grantDbosSchemaPermissions(
  databaseUrl: string,
  roleName: string,
  logger: GlobalLogger,
): Promise<void> {
  logger.info(`Granting permissions for DBOS schema to ${roleName}`);

  const client = new Client(getClientConfig(databaseUrl));
  await client.connect();

  try {
    // Grant usage on the dbos schema
    const grantUsageSql = `GRANT USAGE ON SCHEMA dbos TO "${roleName}"`;
    logger.info(grantUsageSql);
    await client.query(grantUsageSql);

    // Grant all privileges on all existing tables in dbos schema (includes views)
    const grantTablesSql = `GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dbos TO "${roleName}"`;
    logger.info(grantTablesSql);
    await client.query(grantTablesSql);

    // Grant all privileges on all sequences in dbos schema
    const grantSequencesSql = `GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dbos TO "${roleName}"`;
    logger.info(grantSequencesSql);
    await client.query(grantSequencesSql);

    // Grant execute on all functions and procedures in dbos schema
    const grantFunctionsSql = `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbos TO "${roleName}"`;
    logger.info(grantFunctionsSql);
    await client.query(grantFunctionsSql);

    // Grant default privileges for future objects in dbos schema
    const alterTablesSql = `ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT ALL ON TABLES TO "${roleName}"`;
    logger.info(alterTablesSql);
    await client.query(alterTablesSql);

    const alterSequencesSql = `ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT ALL ON SEQUENCES TO "${roleName}"`;
    logger.info(alterSequencesSql);
    await client.query(alterSequencesSql);

    const alterFunctionsSql = `ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT EXECUTE ON FUNCTIONS TO "${roleName}"`;
    logger.info(alterFunctionsSql);
    await client.query(alterFunctionsSql);
  } catch (e) {
    logger.error(`Failed to grant permissions to role ${roleName}: ${(e as Error).message}`);
    throw e;
  } finally {
    await client.end();
  }
}

export async function migrate(
  migrationCommands: string[],
  databaseUrl: string,
  systemDatabaseUrl: string,
  logger: GlobalLogger,
) {
  const url = new URL(databaseUrl);
  const database = url.pathname.slice(1);

  logger.info(`Starting migration: creating database ${database} if it does not exist`);
  await createDBIfDoesNotExist(databaseUrl, logger);

  try {
    migrationCommands?.forEach((cmd) => {
      logger.info(`Executing migration command: ${cmd}`);
      const migrateCommandOutput = execSync(cmd, { encoding: 'utf-8' });
      console.log(migrateCommandOutput.trimEnd());
    });
  } catch (e) {
    logMigrationError(e, logger, 'Error running migration');
    return 1;
  }

  logger.info('Creating DBOS tables and system database.');
  try {
    await ensureSystemDatabase(systemDatabaseUrl, logger);
    await ensureDbosTables(databaseUrl);
  } catch (e) {
    if (e instanceof Error) {
      logger.error(`Error creating DBOS system database: ${e.message}`);
    } else {
      logger.error(e);
    }
    return 1;
  }

  logger.info('Migration successful!');
  return 0;
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
