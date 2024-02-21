import axios , { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, retrieveApplicationName } from "../cloudutils";

export async function getAppLogs(host: string, last: number): Promise<number> {
  if (last != undefined && (isNaN(last) || last <= 0)) {
    throw new Error('The --last parmameter must be an integer greater than 0');
  }
  if (last == undefined) {
    last = 0      //internally, 0 means "get all the logs." This is the default.
  }
  const logger = getLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  const appName = retrieveApplicationName(logger);
  if (appName == null) {
    return 1;
  }
  logger.info(`Retrieving logs for application: ${appName}`)

  try {
    const res = await axios.get(`https://${host}/${userCredentials.userName}/logs/application/${appName}?last=${last}`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      }
    });
    if (res.data == "") {
      logger.info(`No logs found for the specified parameters`);
    } else {
      logger.info(`Successfully retrieved logs of application: ${appName}`);
      logger.info(res.data)
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve logs of application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
