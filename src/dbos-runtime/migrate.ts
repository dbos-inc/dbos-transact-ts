import { execSync } from "child_process";
import { GlobalLogger } from "../telemetry/logs";
import { ConfigFile, constructPoolConfig } from "./config";
import { PoolConfig, Client } from "pg";
import { createUserDBSchema, userDBIndex, userDBSchema } from "../../schemas/user_db_schema";
import { ExistenceCheck, migrateSystemDatabase } from "../system_database";

export async function migrate(configFile: ConfigFile, logger: GlobalLogger) {
  if (!configFile.database.password) {
    logger.error(`DBOS configuration does not contain database password, please check your config file and retry!`);
    return 1;
  }
  const userDBName = configFile.database.app_db_name;
  logger.info(`Starting migration: creating database ${userDBName} if it does not exist`);

  if (!(await checkDatabaseExists(configFile, logger))) {
    const postgresConfig: PoolConfig = constructPoolConfig(configFile)
    postgresConfig.database = "postgres"
    const postgresClient = new Client(postgresConfig);
    let connection_failed = true;
    try {
      await postgresClient.connect()
      connection_failed = false;
      await postgresClient.query(`CREATE DATABASE ${configFile.database.app_db_name}`);
    } catch (e) {
      if (e instanceof Error) {
        if (connection_failed) {
          logger.error(`Error connecting to database ${postgresConfig.host}:${postgresConfig.port} with user ${postgresConfig.user}: ${e.message}`);
        } else {
          logger.error(`Error creating database ${configFile.database.app_db_name}: ${e.message}`);
        }
      } else {
        logger.error(e);
      }
      return 1;
    } finally {
      await postgresClient.end()
    }
  }

  const migrationCommands = configFile.database.migrate;

  try {
    migrationCommands?.forEach((cmd) => {
      logger.info(`Executing migration command: ${cmd}`);
      const migrateCommandOutput = execSync(cmd).toString();
      logger.info(migrateCommandOutput.trimEnd());
    });
  } catch (e) {
    logger.error("Error running migration");
    if (e instanceof Error) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
      const stderr = (e as any).stderr;
      if (stderr && Buffer.isBuffer(stderr) && stderr.length > 0) {
        logger.error(`Standard Error: ${stderr.toString().trim()}`);
      }
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
      const stdout = (e as any).stdout;
      if (stdout && Buffer.isBuffer(stdout) && stdout.length > 0) {
        logger.error(`Standard Output: ${stdout.toString().trim()}`);
      }
      if (e.message) {
        logger.error(e.message);
      }
    } else {
      logger.error(e);
    }
    return 1;
  }

  logger.info("Creating DBOS tables and system database.");
  try {
    await createDBOSTables(configFile);
  } catch (e) {
    if (e instanceof Error) {
      logger.error(`Error creating DBOS system database: ${e.message}`);
    } else {
      logger.error(e);
    }
    return 1;
  }

  logger.info("Migration successful!")
  return 0;
}

export async function checkDatabaseExists(configFile: ConfigFile, logger: GlobalLogger) {
  const pgUserConfig: PoolConfig = constructPoolConfig(configFile)
  const pgUserClient = new Client(pgUserConfig);

  try {
    await pgUserClient.connect(); // Try to establish a connection
    await pgUserClient.end();
    logger.info(`Database ${configFile.database.app_db_name} exists!`)
    return true; // If successful, return true
  } catch (error) {
    logger.info(`Database ${configFile.database.app_db_name} does not exist, creating...`)
    return false; // If connection fails, return false
  }
}

// eslint-disable-next-line @typescript-eslint/require-await
export async function rollbackMigration(configFile: ConfigFile, logger: GlobalLogger) {
  logger.info("Starting Migration Rollback");

  let dbType = configFile.database.app_db_client;
  if (dbType == undefined) {
    dbType = "knex";
  }

  const rollbackcommands = configFile.database.rollback;

  try {
    rollbackcommands?.forEach((cmd) => {
      logger.info("Executing " + cmd);
      execSync(cmd);
    });
  } catch (e) {
    logger.error("Error rolling back migration. ");
    return 1;
  }
  return 0;
}

// Create DBOS system DB and tables.
// TODO: replace this with knex to manage schema.
async function createDBOSTables(configFile: ConfigFile) {
  const logger = new GlobalLogger();

  const userPoolConfig: PoolConfig = constructPoolConfig(configFile)

  const systemPoolConfig = { ...userPoolConfig };
  systemPoolConfig.database = configFile.database.sys_db_name ?? `${userPoolConfig.database}_dbos_sys`;

  const pgUserClient = new Client(userPoolConfig);
  await pgUserClient.connect();

  // Create DBOS table/schema in user DB.
  const schemaExists = await pgUserClient.query<ExistenceCheck>(`SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`);
  if (!schemaExists.rows[0].exists) {
    await pgUserClient.query(createUserDBSchema);
    await pgUserClient.query(userDBSchema);
    await pgUserClient.query(userDBIndex);
  }

  // Create the DBOS system database.
  const dbExists = await pgUserClient.query<ExistenceCheck>(`SELECT EXISTS (SELECT FROM pg_database WHERE datname = '${systemPoolConfig.database}')`);
  if (!dbExists.rows[0].exists) {
    await pgUserClient.query(`CREATE DATABASE ${systemPoolConfig.database}`);
  }

  // Load the DBOS system schema.
  const pgSystemClient = new Client(systemPoolConfig);
  await pgSystemClient.connect();

  try {
    await migrateSystemDatabase(systemPoolConfig);
  } catch (e) {
    const tableExists = await pgSystemClient.query<ExistenceCheck>(`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'operation_outputs')`);
    if (tableExists.rows[0].exists) {
      // If the table has been created by someone else. Ignore the error.
      logger.warn(`System tables creation failed, may conflict with concurrent tasks: ${(e as Error).message}`);
    } else {
      throw e;
    }
  } finally {
    await pgSystemClient.end();
    await pgUserClient.end();
  }
}
