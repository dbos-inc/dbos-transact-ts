import axios , { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse } from "../cloudutils";
import path from "node:path";

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

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  logger.info(`Retrieving logs for application: ${appName}`)

  try {
    const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.userName}/logs/applications/${appName}?last=${last}`, {
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
