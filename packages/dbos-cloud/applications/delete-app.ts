import axios, { AxiosError } from "axios";
import { isCloudAPIErrorResponse, handleAPIErrors, getCloudCredentials, getLogger, retrieveApplicationName, sleep } from "../cloudutils.js";
import { Application } from "./types.js";

export async function deleteApp(host: string, dropdb: boolean, appName?: string): Promise<number> {
  const logger = getLogger()
  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  appName = appName ?? retrieveApplicationName(logger);
  if (!appName) {
    return 1;
  }
  logger.info(`Submitting deletion request for ${appName}`)

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

    logger.info(`Submitted deletion request for ${appName}`)

    // Wait for the application to be deleted
    let count = 0
    let applicationDeleted = false
    while (!applicationDeleted) {
      count += 1
      if (count % 5 === 0) {
        logger.info(`Waiting for ${appName} to be deleted`);
      }
      if (count > 180) {
        logger.error("Application taking too long to be deleted")
        return 1;
      }

      // List all applications, see if the deleted app is among them
      const list = await axios.get(
        `https://${host}/v1alpha1/${userCredentials.userName}/applications`,
        {
          headers: {
            Authorization: bearerToken,
          },
        }
      );
      applicationDeleted = true;
      const applications: Application[] = list.data as Application[];
      for (const application of applications) {
        if (application.Name === appName) {
          applicationDeleted = false
        }
      }
      await sleep(1000);
    }
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
