import axios, { AxiosError } from "axios";
import { getCloudCredentials, getLogger, handleAPIErrors, isCloudAPIErrorResponse, retrieveApplicationName } from "../cloudutils.js";
import { Application } from "./types.js";

export async function updateApp(host: string): Promise<number> {
  const logger =  getLogger();
  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  const appName = retrieveApplicationName(logger);
  if (!appName) {
    return 1;
  }
  logger.info(`Updating application: ${appName}`)

  try {
    logger.info(`Updating application ${appName}`);
    const update = await axios.patch(
      `https://${host}/v1alpha1/${userCredentials.userName}/applications/${appName}`,
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
