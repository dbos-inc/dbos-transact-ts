import axios, { AxiosError } from "axios";
import { isCloudAPIErrorResponse, handleAPIErrors, getCloudCredentials, getLogger, retrieveApplicationName } from "../cloudutils.js";

export async function deleteApp(host: string, dropdb: boolean, appName?: string): Promise<number> {
  const logger = getLogger()
  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  appName = appName ?? retrieveApplicationName(logger);
  if (!appName) {
    return 1;
  }
  logger.info(`Deleting application: ${appName}`)

  try {
    await axios.delete(`https://${host}/v1alpha1/${userCredentials.userName}/applications/${appName}`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
      data: {
        "dropdb": dropdb,
      },
    });

    logger.info(`Successfully deleted application: ${appName}`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to delete application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
