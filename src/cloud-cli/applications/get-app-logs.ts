import axios from "axios";
import { GlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";

export async function getAppLogs(appName: string, host: string, port: string): Promise<number> {
  const logger =  new GlobalLogger();
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
    return 0;
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to retrieve logs of application ${appName}: ${e.response?.data}`);
      return 1;
    } else {
      logger.error(`failed to retrieve logs of application ${appName}: ${(e as Error).message}`);
      return 1;
    }
  }
}
