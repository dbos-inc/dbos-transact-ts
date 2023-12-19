import axios from "axios";
import { GlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";

export async function registerApp(appName: string, dbname: string, host: string, machines: number): Promise<number> {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    const register = await axios.put(
      `https://${host}/${userCredentials.userName}/application`,
      {
        name: appName,
        database: dbname,
        max_vms: machines,
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
