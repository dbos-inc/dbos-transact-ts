import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse } from "../cloudutils";
import { Application } from "./types";

export async function listApps(host: string, json: boolean): Promise<number> {
  const logger = getLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    const list = await axios.get(
      `https://${host}/${userCredentials.userName}/application`,
      {
        headers: {
          Authorization: bearerToken,
        },
      }
    );
    const applications: Application[] = list.data as Application[];
    if (applications.length === 0) {
      logger.info("No applications found");
      return 1;
    }
    if (json) {
      console.log(JSON.stringify(applications));
    } else {
      logger.info(`Listing applications for ${userCredentials.userName}`)
      applications.forEach(app => {
        console.log(`Application Name: ${app.Name}`);
        console.log(`ID: ${app.ID}`);
        console.log(`Database Name: ${app.DatabaseName}`);
        console.log(`Status: ${app.Status}`);
        console.log(`Version: ${app.Version}`);
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
