import axios , { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger } from "../cloudutils";
import path from "node:path";

export async function getAppLogs(host: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  logger.info(`Retrieving logs for application: ${appName}`)

  try {
    const res = await axios.get(`https://${host}/${userCredentials.userName}/logs/application/${appName}`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });

    logger.info(`Successfully retrieved logs of application: ${appName}`);
    logger.info(res.data)
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve logs of application ${appName}`;
    if (axios.isAxiosError(e) && (e as AxiosError).response) {
      handleAPIErrors(errorLabel, e);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
