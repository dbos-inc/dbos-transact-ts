import { Pool, PoolConfig } from "pg";
import { DBOSInitializationError } from "../error";
import { Logger } from "winston";
import Docker from 'dockerode';
import { readFileSync, sleepms } from "../utils";
import { dbosConfigFilePath, writeConfigFile } from "./config";
import YAML from "yaml";
import { DBOSCloudHost, getCloudCredentials, getLogger } from "./cloudutils/cloudutils"
import { chooseAppDBServer, createUserRole, getUserDBCredentials, getUserDBInfo } from "./cloudutils/databases";

export async function db_wizard(poolConfig: PoolConfig): Promise<PoolConfig> {
    const logger = getLogger()

    // 1. Check the connectivity to the database. Return if successful. If cannot connect, continue to the following steps.
    const dbConnectionError = await checkDbConnectivity(poolConfig);
    if (dbConnectionError === null) {
        return poolConfig
    }

    // 2. If the error is due to password authentication or the configuration is non-default, surface the error and exit.
    const errorStr = dbConnectionError.toString();
    if (errorStr.includes('password authentication failed') || errorStr.includes('28P01')) {
        throw new DBOSInitializationError(
            `Could not connect to Postgres: password authentication failed: ${errorStr}`
        );
    }

    if (poolConfig.host !== 'localhost' || poolConfig.port !== 5432 || poolConfig.user !== 'postgres') {
        throw new DBOSInitializationError(
            `Could not connect to the database. Exception: ${errorStr}`
        );
    }

    logger.warn('Postgres not detected locally');

    // 3. If the database config is the default one, check if the user has Docker properly installed.
    logger.info("Attempting to start Postgres via Docker")
    const hasDocker = await checkDockerInstalled()

    // 4. If Docker is installed, prompt the user to start a local Docker based Postgres, and then set the PGPASSWORD to 'dbos' and try to connect to the database.
    let dockerStarted = false;
    if (hasDocker) {
        dockerStarted = await startDockerPostgres(logger, poolConfig)
    } else {
        logger.warn("Docker not detected locally")
    }

    // 5. If no Docker, then prompt the user to log in to DBOS Cloud and provision a DB there. Wait for the remote DB to be ready, and then create a copy of the original config file, and then load the remote connection string to the local config file.
    let localSuffix = false;
    if (!dockerStarted) {
        const cred = await getCloudCredentials(DBOSCloudHost, logger)
        const dbName = await chooseAppDBServer(logger, DBOSCloudHost, cred);
        const db = await getUserDBInfo(DBOSCloudHost, dbName, cred);
        if (!db.IsLinked) {
            await createUserRole(logger, DBOSCloudHost, cred, db.PostgresInstanceName);
        }
        poolConfig.host = db.HostName;
        poolConfig.port = db.Port;
        const dbCredentials = await getUserDBCredentials(logger, DBOSCloudHost, cred, db.PostgresInstanceName);
        poolConfig.user = db.DatabaseUsername;
        poolConfig.password = dbCredentials.Password;
        poolConfig.database = `${poolConfig.database}_local`
        poolConfig.ssl = { rejectUnauthorized: false }
        localSuffix = true;

        const checkError = await checkDbConnectivity(poolConfig);
        if (checkError !== null) {
            throw new DBOSInitializationError(
                `Could not connect to the database. Exception: ${checkError.toString()}`
            );
        }
    }

    // 6. Save the config to the config file and return the updated config.
    const configFileContent = readFileSync(dbosConfigFilePath);
    const config = YAML.parseDocument(configFileContent);
    config.setIn(['database', 'hostname'], poolConfig.host);
    config.setIn(['database', 'port'], poolConfig.port);
    config.setIn(['database', 'username'], poolConfig.user);
    config.setIn(['database', 'password'], poolConfig.password);
    config.setIn(['database', 'local_suffix'], localSuffix);
    writeConfigFile(config, dbosConfigFilePath);

    return poolConfig
}

async function checkDbConnectivity(config: PoolConfig): Promise<Error | null> {
    // Add a default connection timeout if not specified
    const configWithTimeout: PoolConfig = {
        ...config,
        connectionTimeoutMillis: config.connectionTimeoutMillis ?? 2000, // 2 second timeout
        database: "postgres"
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


async function startDockerPostgres(logger: Logger, poolConfig: PoolConfig) {
    logger.info("Starting a Postgres Docker container...");

    const docker = new Docker();
    poolConfig.password = "dbos"
    const containerName = "dbos-db";
    const pgData = "/var/lib/postgresql/data";

    try {
        // Create and start the container
        const container = await docker.createContainer({
            Image: "pgvector/pgvector:pg16",
            name: containerName,
            Env: [
                `POSTGRES_PASSWORD=${poolConfig.password}`,
                `PGDATA=${pgData}`
            ],
            HostConfig: {
                PortBindings: {
                    "5432/tcp": [{ HostPort: poolConfig.port!.toString() }]
                },
                Binds: [`${pgData}:${pgData}:rw`],
                AutoRemove: true
            }
        });

        await container.start();

        // Wait for PostgreSQL to be ready
        let attempts = 30;
        while (attempts > 0) {
            if (attempts % 5 === 0) {
                logger.info("Waiting for Postgres Docker container to start...");
            }
            if (await checkDbConnectivity(poolConfig) === null) {
                return true
            }
            attempts--;
            await sleepms(1000);
        }

        logger.error("Failed to start Postgres Docker container.")
        return false;

    } catch (error) {
        logger.error("Error starting container:", error);
        return false;
    }
}


async function checkDockerInstalled() {
    try {
        const docker = new Docker();
        await docker.ping();
        return true;
    } catch (error) {
        return false;
    }
}