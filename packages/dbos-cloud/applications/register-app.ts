import axios from "axios";
import { GlobalLogger } from "../../../src/telemetry/logs";
import { getCloudCredentials } from "../cloudutils";
import path from "node:path";

export async function registerApp(dbname: string, host: string, machines: number): Promise<number> {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  logger.info(`Loaded application name from package.json: ${appName}`)
  logger.info(`Registering application: ${appName}`)

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
