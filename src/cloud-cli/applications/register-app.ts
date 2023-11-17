import axios from "axios";
import { createGlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";

export async function registerApp(appName: string, host: string, port: string, machines: number): Promise<number> {
  const logger = createGlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    const register = await axios.put(
      `http://${host}:${port}/${userCredentials.userName}/application`,
      {
        name: appName,
        max_vms: machines
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
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to register application ${appName}: ${e.response?.data}`);
      return 1;
    } else {
      logger.error(`failed to register application ${appName}: ${(e as Error).message}`);
      return 1;
    }
  }
}
