import { Pool, PoolConfig } from 'pg';
import { DBOSInitializationError } from '../error';
import { Logger } from 'winston';
import { sleepms } from '../utils';
import { DBOSCloudHost, getCloudCredentials, getLogger } from './cloudutils/cloudutils';
import { chooseAppDBServer, createUserRole, getUserDBCredentials, getUserDBInfo } from './cloudutils/databases';
import { promisify } from 'util';
import { password } from '@inquirer/prompts';
import { exec } from 'child_process';
import { DatabaseConnection, saveDatabaseConnection } from './db_connection';

export async function db_wizard(poolConfig: PoolConfig): Promise<PoolConfig> {
  const logger = getLogger();

  // 1. Check the connectivity to the database. Return if successful. If cannot connect, continue to the following steps.
  const dbConnectionError = await checkDbConnectivity(poolConfig);
  if (dbConnectionError === null) {
    return poolConfig;
  }

  // 2. If the error is due to password authentication or the configuration is non-default, surface the error and exit.
  let errorStr = dbConnectionError.toString();
  if (dbConnectionError instanceof AggregateError) {
    let combinedMessage = 'AggregateError: ';
    for (const error of dbConnectionError.errors) {
      combinedMessage += `${(error as Error).toString()}; `;
    }
    errorStr = combinedMessage;
  }

  if (
    errorStr.includes('password authentication failed') ||
    errorStr.includes('28P01') ||
    errorStr.includes('no password supplied') ||
    errorStr.includes('client password must be a string')
  ) {
    throw new DBOSInitializationError(`Could not connect to Postgres: password authentication failed: ${errorStr}`);
  }

  // If the provided poolConfig database hostname/port/username are set to defaults, skip the wizard.
  if (poolConfig.host !== 'localhost' || poolConfig.port !== 5432 || poolConfig.user !== 'postgres') {
    throw new DBOSInitializationError(`Could not connect to the database. Exception: ${errorStr}`);
  }

  logger.warn('Postgres not detected locally');

  // 3. If the database config is the default one, check if the user has Docker properly installed.
  logger.info('Attempting to start Postgres via Docker');
  const hasDocker = await checkDockerInstalled();

  // 4. If Docker is installed, prompt the user to start a local Docker based Postgres, and then set the PGPASSWORD to 'dbos' and try to connect to the database.
  let dockerStarted = false;
  if (hasDocker) {
    dockerStarted = await startDockerPostgres(logger, poolConfig);
  } else {
    logger.warn('Docker not detected locally');
  }

  // 5. If no Docker, then prompt the user to log in to DBOS Cloud and provision a DB there. Wait for the remote DB to be ready, and then create a copy of the original config file, and then load the remote connection string to the local config file.
  let localSuffix = false;
  if (!dockerStarted) {
    logger.info('Attempting to connect to Postgres via DBOS Cloud');
    const cred = await getCloudCredentials(DBOSCloudHost, logger);
    const dbName = await chooseAppDBServer(logger, DBOSCloudHost, cred);
    const db = await getUserDBInfo(DBOSCloudHost, dbName, cred);
    if (!db.IsLinked) {
      await createUserRole(logger, DBOSCloudHost, cred, db.PostgresInstanceName);
    }
    poolConfig.host = db.HostName;
    poolConfig.port = db.Port;
    if (db.SupabaseReference) {
      poolConfig.user = `postgres.${db.SupabaseReference}`;
      poolConfig.password = await password({
        message: 'Enter your Supabase database password: ',
        mask: '*',
      });
    } else {
      const dbCredentials = await getUserDBCredentials(logger, DBOSCloudHost, cred, db.PostgresInstanceName);
      poolConfig.user = db.DatabaseUsername;
      poolConfig.password = dbCredentials.Password;
    }
    poolConfig.database = `${poolConfig.database}_local`;
    poolConfig.ssl = { rejectUnauthorized: false };
    localSuffix = true;

    const checkError = await checkDbConnectivity(poolConfig);
    if (checkError !== null) {
      throw new DBOSInitializationError(`Could not connect to the database. Exception: ${checkError.toString()}`);
    }
  }

  // 6. Save the config to the database connection file and return the updated config.
  const databaseConnection: DatabaseConnection = {
    hostname: poolConfig.host,
    port: poolConfig.port,
    username: poolConfig.user,
    password: poolConfig.password as string,
    local_suffix: localSuffix,
  };
  saveDatabaseConnection(databaseConnection);

  return poolConfig;
}

async function checkDbConnectivity(config: PoolConfig): Promise<Error | null> {
  // Add a default connection timeout if not specified
  const configWithTimeout: PoolConfig = {
    ...config,
    connectionTimeoutMillis: config.connectionTimeoutMillis ?? 2000, // 2 second timeout
    database: 'postgres',
  };

  const pool = new Pool(configWithTimeout);

  try {
    const client = await pool.connect();
    try {
      await client.query('SELECT 1;');
      return null;
    } catch (error) {
      return error instanceof Error ? error : new Error('Unknown database error');
    } finally {
      client.release();
    }
  } catch (error) {
    return error instanceof Error ? error : new Error('Failed to connect to database');
  } finally {
    // End the pool
    await pool.end();
  }
}

const execAsync = promisify(exec);

async function startDockerPostgres(logger: Logger, poolConfig: PoolConfig): Promise<boolean> {
  logger.info('Starting a Postgres Docker container...');
  poolConfig.password = 'dbos';
  const containerName = 'dbos-db';
  const pgData = '/var/lib/postgresql/data';

  try {
    // Create and start the container
    const dockerCmd = `docker run -d \
            --name ${containerName} \
            -e POSTGRES_PASSWORD=${poolConfig.password} \
            -e PGDATA=${pgData} \
            -p ${poolConfig.port}:5432 \
            -v ${pgData}:${pgData}:rw \
            --rm \
            pgvector/pgvector:pg16`;

    await execAsync(dockerCmd);

    // Wait for PostgreSQL to be ready
    let attempts = 30;
    while (attempts > 0) {
      if (attempts % 5 === 0) {
        logger.info('Waiting for Postgres Docker container to start...');
      }

      if ((await checkDbConnectivity(poolConfig)) === null) {
        return true;
      }

      attempts--;
      await sleepms(1000);
    }

    logger.error('Failed to start Postgres Docker container.');
    return false;
  } catch (error) {
    logger.error('Error starting container:', error);
    return false;
  }
}

async function checkDockerInstalled(): Promise<boolean> {
  try {
    await execAsync('docker --version');
    return true;
  } catch (error) {
    return false;
  }
}
