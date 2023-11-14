import axios from "axios";
import { createGlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";

export async function getAppLogs(appName: string, host: string, port: string) {
  const logger = createGlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    const res = await axios.get(`http://${host}:${port}/${userCredentials.userName}/logs/application/${appName}`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });

    logger.info(`Successfully retrieved logs of application: ${appName}`);
    logger.info(res.data)
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to retrieve logs of application ${appName}: ${e.response?.data}`);
    } else {
      logger.error(`failed to retrieve logs of application ${appName}: ${(e as Error).message}`);
    }
  }
}
