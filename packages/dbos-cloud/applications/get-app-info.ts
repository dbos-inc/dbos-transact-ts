import axios , { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse } from "../cloudutils";
import path from "node:path";
import { Application } from "./types";

export async function getAppInfo(host: string, json: boolean): Promise<number> {
  const logger = getLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  if (!json) {
    logger.info(`Retrieving info for application: ${appName}`)
  }

  try {
    const res = await axios.get(`https://${host}/${userCredentials.userName}/application/${appName}`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      }
    });
    const app = res.data as Application
    if (json) {
      console.log(JSON.stringify(app));
    } else {
      console.log(`Application Name: ${app.Name}`);
      console.log(`ID: ${app.ID}`);
      console.log(`Postgres Instance Name: ${app.DatabaseName}`);
      console.log(`Application Database Name: ${app.DatabaseName}`);
      console.log(`Status: ${app.Status}`);
      console.log(`Version: ${app.Version}`);
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve info for application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
