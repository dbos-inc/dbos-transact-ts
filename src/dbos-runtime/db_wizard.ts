import { Pool, PoolConfig } from "pg";
import { DBOSInitializationError } from "../error";
import { transports, createLogger, format, Logger } from "winston";
import Docker from 'dockerode';
import { sleepms } from "../utils";

export type CLILogger = ReturnType<typeof createLogger>;
let curLogger: Logger | undefined = undefined;
export function getLogger(verbose?: boolean): CLILogger {
    if (curLogger) return curLogger;
    const winstonTransports = [];
    winstonTransports.push(
        new transports.Console({
            format: consoleFormat,
            level: verbose ? "debug" : "info",
        })
    );
    return (curLogger = createLogger({ transports: winstonTransports }));
}

const consoleFormat = format.combine(
    format.errors({ stack: true }),
    format.timestamp(),
    format.colorize(),
    format.printf((info) => {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const { timestamp, level, message, stack } = info;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
        const ts = timestamp.slice(0, 19).replace("T", " ");
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
        const formattedStack = stack?.split("\n").slice(1).join("\n");

        const messageString: string = typeof message === "string" ? message : JSON.stringify(message);

        return `${ts} [${level}]: ${messageString} ${stack ? "\n" + formattedStack : ""}`;
    })
);

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

    if (!dockerStarted) {
        throw new DBOSInitializationError("Cloud not done yet");
    }

    // 6. Save the config to the config file and return the updated config.
    // TODO: make the config file prettier

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