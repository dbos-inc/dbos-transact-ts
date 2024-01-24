import axios from "axios";
import { getCloudCredentials, getLogger } from "../cloudutils";
import { Application } from "./types";
import path from "node:path";

export async function updateApp(host: string, machines: number): Promise<number> {
  const logger =  getLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  logger.info(`Loaded application name from package.json: ${appName}`)
  logger.info(`Updating application: ${appName}`)

  try {
    logger.info(`Updating application ${appName} to ${machines} machines`);
    const update = await axios.patch(
      `https://${host}/${userCredentials.userName}/application/${appName}`,
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
    const application: Application = update.data as Application;
    logger.info(`Successfully updated: ${application.Name}`);
    console.log(JSON.stringify({ "Name": application.Name, "ID": application.ID, "Version": application.Version, "MaxVMs": application.MaxVMs }));
    return 0;
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`Failed to update application ${appName}: ${e.response?.data}`);
      return 1;
    } else {
      (e as Error).message = `Failed to update application ${appName}: ${(e as Error).message}`;
      logger.error(e);
      return 1;
    }
  }
}
