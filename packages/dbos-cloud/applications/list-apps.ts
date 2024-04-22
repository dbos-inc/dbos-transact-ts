import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse } from "../cloudutils.js";
import { Application, prettyPrintApplication } from "./types.js";

export async function listApps(host: string, json: boolean): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    const list = await axios.get(
      `https://${host}/v1alpha1/${userCredentials.userName}/applications`,
      {
        headers: {
          Authorization: bearerToken,
        },
      }
    );
    const applications: Application[] = list.data as Application[];
    if (json) {
      console.log(JSON.stringify(applications));
    } else {
      if (applications.length === 0) {
        logger.info("No applications found");
      }
      applications.forEach(app => {
        prettyPrintApplication(app);
        console.log('-------------------------');
      });
    }
    return 0;
  } catch (e) {
    const errorLabel = 'Failed to list applications';
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
