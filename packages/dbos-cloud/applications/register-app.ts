import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse } from "../cloudutils";
import path from "node:path";

export async function registerApp(dbname: string, host: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  logger.info(`Loaded application name from package.json: ${appName}`)
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
