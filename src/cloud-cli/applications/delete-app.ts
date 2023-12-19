import axios from "axios";
import { GlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";

export async function deleteApp(appName: string, host: string): Promise<number> {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

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
