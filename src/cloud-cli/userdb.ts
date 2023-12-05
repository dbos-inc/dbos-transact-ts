import axios from "axios";
import { createGlobalLogger } from "../telemetry/logs";
import { getCloudCredentials } from "./utils";
import { sleep } from "../utils"
import { ConfigFile, loadConfigFile, dbosConfigFilePath } from "../dbos-runtime/config";
import { execSync } from "child_process";

export async function createUserDb(host: string, port: string, dbName: string, adminName: string, adminPassword: string, sync: boolean) {
  const logger = createGlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  
  try {
    await axios.post(`http://${host}:${port}/${userCredentials.userName}/databases/userdb`, 
    {"Name": dbName,"AdminName": adminName, "AdminPassword": adminPassword},
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });

    logger.info(`Successfully started creating database: ${dbName}`);
    
    if(sync) {
      let status = ""
      let data
      while (status != "available") {
        await sleep(60000)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        data = await getDb(host, port, dbName)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        logger.info(data)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
        status = data.Status
      }

      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
      const dbhostname = data.HostName ;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
      const dbport = data.Port;

      // Update the clouddb info record
      logger.info("Saving db state to cloud db");
      await axios.put(`http://${host}:${port}/${userCredentials.userName}/databases/userdb/info`, 
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      {"DBName": dbName,"Status": status, "HostName": dbhostname, "Port": dbport},
      {
        headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
      });

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
  const logger = createGlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    await axios.delete(`http://${host}:${port}/${userCredentials.userName}/databases/userdb/${dbName}`, 
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });

    logger.info(`Successfully started deleting database: ${dbName}`);
    if(sync) {
      let status = "deleting"
      while (status == "deleting") {
        await sleep(60000)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
          let data
          try {
            // HACK to exit gracefully because the get throws an exception on 500
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
            data = await getDb(host, port, dbName)
          } catch(e) {
            logger.info(`Deleted database: ${dbName}`);
            break;
          }
         // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
          logger.info(data)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
          status = data.Status
      }

      // Update the clouddb info record
      logger.info("Saving db state to cloud db");
      await axios.put(`http://${host}:${port}/${userCredentials.userName}/databases/userdb/info`, 
      {"Name": dbName,"Status": "deleted", "HostName": "", "Port": 0},
      {
        headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
      });


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
  const logger = createGlobalLogger();

  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const res = await getDb(host, port, dbName)
    logger.info(res)
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`Error getting database ${dbName}: ${e.response?.data}`);
    } else {
      logger.error(`Error getting database ${dbName}: ${(e as Error).message}`);
    }
  }
}

export function migrate() {
  const logger = createGlobalLogger();

  // read the yaml file
  const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
  if (!configFile) {
    logger.error(`failed to parse ${dbosConfigFilePath}`);
    return;
  }

  let dbType = configFile.database.user_dbclient;
  if (dbType == undefined) {
    dbType = 'knex';
  }

  const migratecommands = configFile.database.migrate;
  const rollbackcommands = configFile.database.rollback;
  
  try {

    migratecommands?.forEach((cmd) => {
        const command = "npx "+ dbType + " " + cmd; 
        logger.info("Executing " + command);
        execSync(command);
    })
  } catch(e) {
    const err: Error = e as Error;
    logger.error(err);

    rollbackcommands?.forEach((cmd) => {
      const command = "npx " + dbType + " " + cmd; 
        logger.info("Executing " + command);
        execSync(command);
    })

  }

}

// eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-explicit-any
async function getDb(host: string, port: string, dbName: string) : Promise<any> {

  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  
  const res = await axios.get(`http://${host}:${port}/${userCredentials.userName}/databases/userdb/${dbName}`, 
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-enum-comparison
   if (res.status == axios.HttpStatusCode.Ok) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
     return res.data 
   } else {
     return {"Status" : "notfound"}
   }
}
