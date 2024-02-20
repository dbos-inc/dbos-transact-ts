import axios, { AxiosError } from "axios";
import { getCloudCredentials, getLogger, handleAPIErrors, isCloudAPIErrorResponse } from "../cloudutils";
import { Application } from "./types";
import path from "node:path";

export async function updateApp(host: string): Promise<number> {
  const logger =  getLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  logger.info(`Loaded application name from package.json: ${appName}`)
  if (appName === undefined) {
    logger.error("Error: package.json not found. Please run this command in an application root directory.")
    return 1;
  }
  logger.info(`Updating application: ${appName}`)

  try {
    logger.info(`Updating application ${appName}`);
    const update = await axios.patch(
      `https://${host}/${userCredentials.userName}/application/${appName}`,
      {
        name: appName,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );
    const application: Application = update.data as Application;
    logger.info(`Successfully updated: ${application.Name}`);
    return 0;
  } catch (e) {
    const errorLabel = "Failed to update application";
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
