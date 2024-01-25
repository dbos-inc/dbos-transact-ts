import { execSync } from "child_process";
import { GlobalLogger } from "../telemetry/logs";
import { UserDatabaseName } from "../user_database";
import { ConfigFile, dbosConfigFilePath, loadConfigFile } from "./config";
import { readFileSync } from "../utils";
import { PoolConfig, Client } from "pg";
import { createUserDBSchema, userDBSchema } from "../../schemas/user_db_schema";
import { ExistenceCheck, migrateSystemDatabase } from "../system_database";

import { TelemetryCollector } from '../telemetry/collector';
import { TelemetryConfig } from '../telemetry';
import { TelemetryExporter } from '../telemetry/exporters';
import { getConfigFileParsingDiagnostics } from "typescript";

async function sleep(millis: number) {
  return new Promise((resolve) => setTimeout(resolve, millis));
}

export async function bugRepro() {
  const configFile = loadConfigFile(dbosConfigFilePath);
  if (!configFile) {
    console.log("Wrong branch")
    process.exit(1);
  }
  if (configFile.telemetry?.OTLPExporter) {
    const telemetryCollector = new TelemetryCollector(new TelemetryExporter(configFile.telemetry.OTLPExporter));
    const logger = new GlobalLogger(telemetryCollector, configFile.telemetry?.logs)
    /////////////////////////////////
    // These 3 lines make things work:
    //logger.info("Logging some data")
    //telemetryCollector.processAndExportSignals();
    //await sleep(5); 
    /////////////////////////////////
    
    logger.info("Exiting now"); 
    await telemetryCollector.destroy();
    console.log("Exiting")
    process.exit(1);
  }
  console.log("Wrong branch")
  process.exit(1);
}


function initConfig(): [ConfigFile, GlobalLogger, (code: number) => void] {
  let logger = new GlobalLogger();
  const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
  if (!configFile) {
    logger.error(`Failed to parse ${dbosConfigFilePath}`);
    process.exit(1);
  }
  if (configFile.telemetry?.OTLPExporter) {
    const telemetryCollector = new TelemetryCollector(new TelemetryExporter(configFile.telemetry.OTLPExporter));
    logger = new GlobalLogger(telemetryCollector, configFile.telemetry?.logs)
    function terminate(code: number) {
      console.log("Terminating")
      telemetryCollector.destroy().finally(() => {
        console.log("Exiting")
        process.exit(code);
      });
    }
    logger.info("Exporter Initialized");
    telemetryCollector.processAndExportSignals() //needs to be present
    return [ configFile, logger, terminate ];    
  }  else {
    function terminate(code: number) {
      process.exit(code);
    }
    return [ configFile, logger, terminate ];
  }
}

export async function runAndLog( action: (configFile: ConfigFile, logger:GlobalLogger) => Promise<number>) {
  let [configFile, logger, terminate] = initConfig();
  const returnCode = await action(configFile, logger);
  terminate(returnCode);
}

export async function migrate(configFile: ConfigFile, logger:GlobalLogger) {
  const userDBName = configFile.database.user_database;
  //logger.info("Starting Database Migration");
  //logger.info(`Creating database ${userDBName} if it does not already exist`);
  await sleep(5);  //needs to be present
  if (userDBName == 'hello') {
    logger.error('Injected Error, Buddy!');
    return 1;
  }

  const createDB = `createdb -h ${configFile.database.hostname} -p ${configFile.database.port} ${userDBName} -U ${configFile.database.username} -ew ${userDBName}`;
  try {
    process.env.PGPASSWORD = configFile.database.password;
    const createDBOutput = execSync(createDB, { env: process.env }).toString();
    if (createDBOutput.includes(`database "${userDBName}" already exists`)) {
      logger.info(`Database ${userDBName} already exists`);
    } else {
      logger.info(createDBOutput);
    }
  } catch (e) {
    if (e instanceof Error) {
      if (e.message.includes(`database "${userDBName}" already exists`)) {
        logger.info(`Database ${userDBName} already exists`);
      } else {
        logger.error(`Error creating database: ${e.message}`);
        return 1;
      }
    } else {
      logger.error(e);
      return 1;
    }
  }

  const dbType = configFile.database.user_dbclient || UserDatabaseName.KNEX;
  const migrationScript = `node_modules/.bin/${dbType}`;
  const migrationCommands = configFile.database.migrate;

  try {
    migrationCommands?.forEach((cmd) => {
      const command = `node ${migrationScript} ${cmd}`;
      logger.info(`Executing migration command: ${command}`);
      const migrateCommandOutput = execSync(command).toString();
      logger.info(migrateCommandOutput);
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

  return 0;
}

export async function rollbackMigration(configFile: ConfigFile, logger:GlobalLogger) {
  logger.info("Starting Database Migration Rollback");
  
  let dbType = configFile.database.user_dbclient;
  if (dbType == undefined) {
    dbType = "knex";
  }

  const rollbackcommands = configFile.database.rollback;

  try {
    rollbackcommands?.forEach((cmd) => {
      const command = "npx " + dbType + " " + cmd;
      logger.info("Executing " + command);
      execSync(command);
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

  const userPoolConfig: PoolConfig = {
    host: configFile.database.hostname,
    port: configFile.database.port,
    user: configFile.database.username,
    password: configFile.database.password,
    connectionTimeoutMillis: configFile.database.connectionTimeoutMillis || 3000,
    database: configFile.database.user_database,
  };

  if (configFile.database.ssl_ca) {
    userPoolConfig.ssl = { ca: [readFileSync(configFile.database.ssl_ca)], rejectUnauthorized: true };
  }

  const systemPoolConfig = { ...userPoolConfig };
  systemPoolConfig.database = `${userPoolConfig.database}_dbos_sys`;

  const pgUserClient = new Client(userPoolConfig);
  await pgUserClient.connect();

  // Create DBOS table/schema in user DB.
  const schemaExists = await pgUserClient.query<ExistenceCheck>(`SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`);
  if (!schemaExists.rows[0].exists) {
    await pgUserClient.query(createUserDBSchema);
    await pgUserClient.query(userDBSchema);
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
    await migrateSystemDatabase(systemPoolConfig)
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
