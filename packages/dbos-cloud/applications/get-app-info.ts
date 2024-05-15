import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, retrieveApplicationName } from "../cloudutils.js";
import { Application, prettyPrintApplication } from "./types.js";

export async function getAppInfo(host: string, json: boolean, appName?: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  appName = appName ?? retrieveApplicationName(logger, json);
  if (!appName) {
    return 1;
  }
  if (!json) {
    logger.info(`Retrieving info for application: ${appName}`);
  }

  try {
    const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    const app = res.data as Application;
    if (json) {
      console.log(JSON.stringify(app));
    } else {
      prettyPrintApplication(app);
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
