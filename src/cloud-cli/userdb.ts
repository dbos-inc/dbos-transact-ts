import axios from "axios";
import { createGlobalLogger } from "../telemetry/logs";
import { getCloudCredentials } from "./utils";
import { JsonObject } from "@prisma/client/runtime/library";

export async function createUserDb(host: string, port: string, dbName: string, adminName: string, adminPassword: string, sync: boolean) {
  const logger = createGlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  
  try {
    const res = await axios.post(`http://${host}:${port}/${userCredentials.userName}/databases/userdb`, 
    {"Name": dbName,"AdminName": adminName, "AdminPassword": adminPassword},
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });

    logger.info(`Successfully started creating database: ${dbName}`);
    var status = ""
    if(sync) {

      while (status != "available") {
        await sleep(60000)
        const data = await getDb(host, port, dbName)
        logger.info(data)
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

export async function deleteUserDb(host: string, port: string, dbName: string) {
  const logger = createGlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    const res = await axios.delete(`http://${host}:${port}/${userCredentials.userName}/databases/userdb/${dbName}`, 
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });

    logger.info(`Successfully started deleting database: ${dbName}`);
    // logger.info(res.data)
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
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
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

   return res.data 
}

async function sleep(ms: number): Promise<void> {
  return new Promise(
      (resolve) => setTimeout(resolve, ms));
}