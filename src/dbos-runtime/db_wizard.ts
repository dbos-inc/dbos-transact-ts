import { Pool, PoolConfig } from 'pg';
import { DBOSInitializationError } from '../error';
import { Logger } from 'winston';
import { sleepms } from '../utils';
import { getLogger } from './cloudutils/cloudutils';
import { promisify } from 'util';
import { exec } from 'child_process';

export async function db_wizard(poolConfig: PoolConfig) {
  const logger = getLogger();

  // 1. Check the connectivity to the database. Return if successful. If cannot connect, continue to the following steps.
  const dbConnectionError = await checkDbConnectivity(poolConfig);
  if (dbConnectionError === null) {
    logger.info(
      `Postgres is already available locally at postgres://${poolConfig.user}:***@${poolConfig.host}:${poolConfig.port}`,
    );
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

  // If the provided poolConfig database hostname/port/username/password are not the defaults, skip the wizard.
  if (
    poolConfig.host !== 'localhost' ||
    poolConfig.port !== 5432 ||
    poolConfig.user !== 'postgres' ||
    poolConfig.password !== 'dbos'
  ) {
    throw new DBOSInitializationError(`Could not connect to the database. Exception: ${errorStr}`);
  }

  // 3. If the database config is the default one, check if the user has Docker properly installed.
  logger.info('Attempting to start Postgres via Docker');
  const hasDocker = await checkDockerInstalled();

  // 4. If Docker is installed, prompt the user to start a local Docker based Postgres, and then set the PGPASSWORD to 'dbos' and try to connect to the database.
  if (hasDocker) {
    await startDockerPostgres(logger, poolConfig);
    logger.info(
      `Postgres available at postgres://postgres:${poolConfig.password}@${poolConfig.host}:${poolConfig.port}`,
    );
  } else {
    logger.warn('Docker not detected locally');
  }
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
