import { Pool, PoolConfig } from 'pg';
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

export async function startDockerPg() {
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

  throw new Error(`Failed to start Docker container: Container ${containerName} did not start in time.`);
}

async function checkDockerInstalled(): Promise<boolean> {
  try {
    await execAsync('docker --version');
    return true;
  } catch (error) {
    return false;
  }
}

/**
 * Stops the Docker Postgres container.
 *
 * @returns {Promise<boolean>} True if the container was successfully stopped, false if it wasn't running
 * @throws {Error} If there was an error stopping the container
 */
export async function stopDockerPg(): Promise<boolean> {
  const logger = getLogger();
  const containerName = 'dbos-db';

  try {
    logger.info(`Stopping Docker Postgres container ${containerName}...`);

    // Check if container exists and is running
    const { stdout: containerStatus } = await execAsync(`docker ps -a -f name=${containerName} --format "{{.Status}}"`);

    if (!containerStatus) {
      logger.info(`Container ${containerName} does not exist.`);
      return false;
    }

    const isRunning = containerStatus.toLowerCase().includes('up');

    if (!isRunning) {
      logger.info(`Container ${containerName} exists but is not running.`);
      return false;
    }

    // Stop the container
    await execAsync(`docker stop ${containerName}`);
    logger.info(`Successfully stopped Docker Postgres container ${containerName}.`);
    return true;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    logger.error(`Failed to stop Docker Postgres container: ${errorMessage}`);
    throw error;
  }
}
