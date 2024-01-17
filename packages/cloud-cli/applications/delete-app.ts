import axios from "axios";
import { GlobalLogger } from "telemetry/logs";
import { getCloudCredentials } from "../utils";
import path from "node:path";

export async function deleteApp(host: string): Promise<number> {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  logger.info(`Loaded application name from package.json: ${appName}`)
  logger.info(`Deleting application: ${appName}`)

  try {
    await axios.delete(`https://${host}/${userCredentials.userName}/application/${appName}`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });

    logger.info(`Successfully deleted application: ${appName}`);
    return 0;
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to delete application ${appName}: ${e.response?.data}`);
      return 1;
    } else {
      logger.error(`failed to delete application ${appName}: ${(e as Error).message}`);
      return 1;
    }
  }
}
