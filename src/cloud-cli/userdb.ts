import axios from "axios";
import { GlobalLogger } from "../telemetry/logs";
import { getCloudCredentials } from "./utils";
import { sleep } from "../utils";
import { ConfigFile, loadConfigFile, dbosConfigFilePath } from "../dbos-runtime/config";
import { execSync } from "child_process";
import { UserDatabaseName } from "../user_database";

export interface UserDBInstance {
  readonly DBName: string,
  readonly UserID: string,
  readonly Status: string,
  readonly HostName: string,
  readonly Port: number,
}

export async function createUserDb(host: string, port: string, dbName: string, adminName: string, adminPassword: string, sync: boolean) {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    await axios.post(
      `http://${host}:${port}/${userCredentials.userName}/databases/userdb`,
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
        const userDBInfo = await getUserDBInfo(host, port, dbName);
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

export async function deleteUserDb(host: string, port: string, dbName: string) {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    await axios.delete(`http://${host}:${port}/${userCredentials.userName}/databases/userdb/${dbName}`, {
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

export async function getUserDb(host: string, port: string, dbName: string) {
  const logger = new GlobalLogger();

  try {
    const userDBInfo = await getUserDBInfo(host, port, dbName);
    logger.info(userDBInfo);
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`Error getting database ${dbName}: ${e.response?.data}`);
    } else {
      logger.error(`Error getting database ${dbName}: ${(e as Error).message}`);
    }
  }
}

export function migrate(): number {
  const logger = new GlobalLogger();

  // Read the configuration YAML file
  const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
  if (!configFile) {
    logger.error(`Failed to parse ${dbosConfigFilePath}`);
    return 1;
  }

  const userDBName = configFile.database.user_database;

  logger.info(`Creating database ${userDBName} if it does not already exist`);
  const createDB = `PGPASSWORD=${configFile.database.password} createdb -h ${configFile.database.hostname} -p ${configFile.database.port} ${userDBName} -U ${configFile.database.username} -ew ${userDBName}`;
  try {
    const createDBOutput = execSync(createDB).toString();
    if (createDBOutput.includes(`database "${userDBName}" already exists`)) {
      logger.info(`Database ${userDBName} already exists`)
    } else {
      logger.info(createDBOutput)
    }
  } catch (e) {
    if (e instanceof Error) {
      logger.error(`Error creating database: ${e.message}`);
    }
  }

  const dbType = configFile.database.user_dbclient || UserDatabaseName.KNEX;
  const migrationScript = `node_modules/.bin/${dbType}`
  const migrationCommands = configFile.database.migrate;

  try {
    migrationCommands?.forEach((cmd) => {
      const command = `node ${migrationScript} ${cmd}`
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

export async function getUserDBInfo(host: string, port: string, dbName: string): Promise<UserDBInstance> {
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  const res = await axios.get(`http://${host}:${port}/${userCredentials.userName}/databases/userdb/info/${dbName}`, {
    headers: {
      "Content-Type": "application/json",
      Authorization: bearerToken,
    },
  });

  return res.data as UserDBInstance;
}
