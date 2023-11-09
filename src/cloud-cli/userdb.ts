import axios from "axios";
import { createGlobalLogger } from "../telemetry/logs";
import { getCloudCredentials } from "./utils";
import { sleep } from "../utils"

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
      while (status != "available") {
        await sleep(60000)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const data = await getDb(host, port, dbName)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        logger.info(data)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
        status = data.Status
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
          var data
          try {
            // HACK to exit gracefully because the get throws an exception on 500
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

    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
   if (res.status == axios.HttpStatusCode.Ok) {
     return res.data 
   } else {
     return {"Status" : "notfound"}
   }
}
