import axios, { AxiosError } from "axios";
import { isCloudAPIErrorResponse, handleAPIErrors, getCloudCredentials, getLogger } from "./cloudutils";
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
    return 0;
  } catch (e) {
    const errorLabel = `Failed to create database ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
        handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
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
    return 0;
  } catch (e) {
    const errorLabel = `Failed to delete database ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
        handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function getUserDb(host: string, dbName: string, json: boolean) {
  const logger = getLogger();

  try {
    const userDBInfo = await getUserDBInfo(host, dbName);
    if (json) {
      console.log(JSON.stringify(userDBInfo));
    } else {
      logger.info(`Retrieving status of: ${dbName}`);
      console.log(`DB Name: ${userDBInfo.DBName}`);
      console.log(`Status: ${userDBInfo.Status}`);
      console.log(`Host Name: ${userDBInfo.HostName}`);
      console.log(`Port: ${userDBInfo.Port}`);
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retreive database record ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
        handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
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

  // TODO: this needs a type guard
  return res.data as UserDBInstance;
}
