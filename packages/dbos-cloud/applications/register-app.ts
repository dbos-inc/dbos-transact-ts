import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, retrieveApplicationName } from "../cloudutils";

export async function registerApp(dbname: string, host: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  const appName = retrieveApplicationName(logger);
  if (appName === null) {
    return 1;
  }
  logger.info(`Registering application: ${appName}`)

  try {
    const register = await axios.put(
      `https://${host}/v1alpha1/${userCredentials.userName}/applications`,
      {
        name: appName,
        database: dbname,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );
    const uuid = register.data as string;
    logger.info(`Successfully registered: ${appName}`);
    logger.info(`${appName} ID: ${uuid}`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to register application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
