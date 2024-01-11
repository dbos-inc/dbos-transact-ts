import axios from "axios";
import { GlobalLogger } from "../telemetry/logs";
import { getCloudCredentials } from "./utils";
import { readFileSync, sleep } from "../utils";
import { ConfigFile, loadConfigFile, dbosConfigFilePath } from "../dbos-runtime/config";
import { execSync } from "child_process";
import { UserDatabaseName } from "../user_database";
import { Client, PoolConfig } from "pg";
import { ExistenceCheck } from "../system_database";
import { systemDBSchema } from "../../schemas/system_db_schema";
import { createUserDBSchema, userDBSchema } from "../../schemas/user_db_schema";

export interface UserDBInstance {
  readonly DBName: string;
  readonly Status: string;
  readonly HostName: string;
  readonly Port: number;
}

export async function createUserDb(host: string, dbName: string, adminName: string, adminPassword: string, sync: boolean) {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    await axios.post(
      `https://${host}/${userCredentials.userName}/databases/userdb`,
      { Name: dbName, AdminName: adminName, AdminPassword: adminPassword },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );

    logger.info(`Successfully started creating database: ${dbName}`);

    if (sync) {
      let status = "";
      while (status != "available") {
        await sleep(30000);
        const userDBInfo = await getUserDBInfo(host, dbName);
        logger.info(userDBInfo);
        status = userDBInfo.Status;
      }
    }
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`Error creating database ${dbName}: ${e.response?.data}`);
    } else {
      logger.error(`Error creating database ${dbName}: ${(e as Error).message}`);
    }
  }
}

export async function deleteUserDb(host: string, dbName: string) {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    await axios.delete(`https://${host}/${userCredentials.userName}/databases/userdb/${dbName}`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    logger.info(`Database deleted: ${dbName}`);
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`Error deleting database ${dbName}: ${e.response?.data}`);
    } else {
      logger.error(`Error deleting database ${dbName}: ${(e as Error).message}`);
    }
  }
}

export async function getUserDb(host: string, dbName: string) {
  const logger = new GlobalLogger();

  try {
    const userDBInfo = await getUserDBInfo(host, dbName);
    logger.info(userDBInfo);
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`Error getting database ${dbName}: ${e.response?.data}`);
    } else {
      logger.error(`Error getting database ${dbName}: ${(e as Error).message}`);
    }
  }
}

export async function migrate(): Promise<number> {
  const logger = new GlobalLogger();

  // Read the configuration YAML file
  const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
  if (!configFile) {
    logger.error(`Failed to parse ${dbosConfigFilePath}`);
    return 1;
  }

  const userDBName = configFile.database.user_database;

  logger.info(`Creating database ${userDBName} if it does not already exist`);
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

export function rollbackMigration(): number {
  const logger = new GlobalLogger();

  // read the yaml file
  const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
  if (!configFile) {
    logger.error(`failed to parse ${dbosConfigFilePath}`);
    return 1;
  }

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

export async function getUserDBInfo(host: string, dbName: string): Promise<UserDBInstance> {
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  const res = await axios.get(`https://${host}/${userCredentials.userName}/databases/userdb/info/${dbName}`, {
    headers: {
      "Content-Type": "application/json",
      Authorization: bearerToken,
    },
  });

  return res.data as UserDBInstance;
}

// Create DBOS system DB and tables.
// TODO: replace this with knex to manage schema.
async function createDBOSTables(configFile: ConfigFile) {
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
  systemPoolConfig.database = "dbos_systemdb"; // We enforce it to be the dbos_systemdb.

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
  await pgSystemClient.query("BEGIN");
  const tableExists = await pgSystemClient.query<ExistenceCheck>(`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'operation_outputs')`);
  if (!tableExists.rows[0].exists) {
    await pgSystemClient.query(systemDBSchema);
  }
  await pgSystemClient.query("COMMIT");
}
