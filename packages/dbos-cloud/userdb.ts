import axios from "axios";
import { getCloudCredentials, getLogger } from "./cloudutils";
import { sleep } from "../../src/utils";

export interface UserDBInstance {
  readonly DBName: string;
  readonly Status: string;
  readonly HostName: string;
  readonly Port: number;
}

export async function createUserDb(host: string, dbName: string, adminName: string, adminPassword: string, sync: boolean) {
  const logger = getLogger();
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
      while (status != "available" && status != "backing-up") {
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
  const logger = getLogger();
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
  const logger = getLogger();

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
