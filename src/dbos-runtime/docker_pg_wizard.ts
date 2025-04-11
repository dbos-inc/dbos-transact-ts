import { Pool, PoolConfig } from 'pg';
import { DBOSInitializationError } from '../error';
import { Logger } from 'winston';
import { sleepms } from '../utils';
import { getLogger } from './cloudutils/cloudutils';
import { promisify } from 'util';
import { exec } from 'child_process';

/**
 * Helper to create a dockerized Postgres database if the user has not already set up a local Postgres database.
 *
 * @returns null
 */

export async function dockerPgWizard() {
  const logger = getLogger();
  logger.info('Attempting to create a Docker Postgres container...');

  const hasDocker = await checkDockerInstalled();

  const poolConfig = {
    host: 'localhost',
    port: 5432,
    password: process.env.PGPASSWORD || 'dbos',
    user: 'postgres',
    database: 'postgres',
    connectionTimeoutMillis: 2000,
  };

  // If Docker is installed, prompt the user to start a local Docker based Postgres, and then set the PGPASSWORD to 'dbos' and try to connect to the database.
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
  const pool = new Pool(config);

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
  const containerName = 'dbos-db';
  const pgData = '/var/lib/postgresql/data';

  try {
    // Create and start the container
    const dockerCmd = `docker run -d \
            --name ${containerName} \
            -e POSTGRES_PASSWORD=${poolConfig.password as string} \
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

    throw new DBOSInitializationError(
      `Failed to start Docker container: Container ${containerName} did not start in time.`,
    );
  } catch (error) {
    throw new DBOSInitializationError(
      `Failed to start Docker container: ${error instanceof Error ? error.message : 'Unknown error'}`,
    );
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
