import axios from "axios";
import { GlobalLogger } from "../telemetry/logs";
import { getCloudCredentials } from "./utils";
import { sleep } from "../utils";
import { ConfigFile, loadConfigFile, dbosConfigFilePath } from "../dbos-runtime/config";
import { execSync } from "child_process";

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
      let data;
      while (status != "available") {
        await sleep(60000);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        data = await getDb(host, port, dbName);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        logger.info(data as string);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
        status = data.Status;
      }

      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
      const dbhostname = data.HostName;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
      const dbport = data.Port;

      // Update the clouddb info record
      logger.info("Saving db state to cloud db");
      await axios.put(
        `http://${host}:${port}/${userCredentials.userName}/databases/userdb/info`,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        { DBName: dbName, Status: status, HostName: dbhostname, Port: dbport },
        {
          headers: {
            "Content-Type": "application/json",
            Authorization: bearerToken,
          },
        }
      );
    }
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`Error creating database ${dbName}: ${e.response?.data}`);
    } else {
      logger.error(`Error creating database ${dbName}: ${(e as Error).message}`);
    }
  }
}

export async function deleteUserDb(host: string, port: string, dbName: string, sync: boolean) {
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

    logger.info(`Successfully started deleting database: ${dbName}`);
    if (sync) {
      let status = "deleting";
      while (status == "deleting") {
        await sleep(60000);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        let data;
        try {
          // HACK to exit gracefully because the get throws an exception on 500
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
          data = await getDb(host, port, dbName);
        } catch (e) {
          logger.info(`Deleted database: ${dbName}`);
          break;
        }
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        logger.info(data as string);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
        status = data.Status;
      }

      // Update the clouddb info record
      logger.info("Saving db state to cloud db");
      await axios.put(
        `http://${host}:${port}/${userCredentials.userName}/databases/userdb/info`,
        { Name: dbName, Status: "deleted", HostName: "", Port: 0 },
        {
          headers: {
            "Content-Type": "application/json",
            Authorization: bearerToken,
          },
        }
      );
    }
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
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const res = await getDb(host, port, dbName);
    logger.info(res as string);
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

  // read the yaml file
  const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
  if (!configFile) {
    logger.error(`failed to parse ${dbosConfigFilePath}`);
    return 1;
  }

  let create_db = configFile.database.create_db;
  if (create_db == undefined) {
    create_db = false;
  }

  const userdbname = configFile.database.user_database;

  if (create_db) {
    logger.info(`Creating database ${userdbname}`);
    const cmd = `PGPASSWORD=${configFile.database.password} createdb -h ${configFile.database.hostname} -p ${configFile.database.port} ${userdbname} -U ${configFile.database.username} -ew ${userdbname}`;
    logger.info(cmd);
    try {
      const createDBCommandOutput = execSync(cmd).toString();
      logger.info(createDBCommandOutput);
    } catch (e) {
      if (e instanceof Error && !e.message.includes(`database "${userdbname}" already exists`)) {
        logger.error(`Error creating database: ${e.message}`);
      }
    }
  }

  let dbType = configFile.database.user_dbclient;
  if (dbType === undefined) {
    dbType = "knex";
  }

  const migratecommands = configFile.database.migrate;

  try {
    migratecommands?.forEach((cmd) => {
      const command = "npx " + dbType + " " + cmd;
      logger.info("Executing " + command);
      const migrateCommandOutput = execSync(command).toString();
      logger.info(migrateCommandOutput);
    });
  } catch (e) {
    logger.error("Error running migration. Check database and if necessary, run npx dbos-cloud userdb rollback.");
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

export function rollbackmigration(): number {
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

// eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-explicit-any
async function getDb(host: string, port: string, dbName: string): Promise<any> {
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  const res = await axios.get(`http://${host}:${port}/${userCredentials.userName}/databases/userdb/${dbName}`, {
    headers: {
      "Content-Type": "application/json",
      Authorization: bearerToken,
    },
  });

  // eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-enum-comparison
  if (res.status == axios.HttpStatusCode.Ok) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return res.data;
  } else {
    return { Status: "notfound" };
  }
}
