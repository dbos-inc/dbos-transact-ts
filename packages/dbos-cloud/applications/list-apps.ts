import axios from "axios";
import { GlobalLogger } from "../../../src/telemetry/logs";
import { getCloudCredentials } from "../cloudutils";
import { Application } from "./types";

export async function listApps(host: string): Promise<number> {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    const list = await axios.get(
      `https://${host}/${userCredentials.userName}/application`,
      {
        headers: {
          Authorization: bearerToken,
        },
      }
    );
    const data: Application[] = list.data as Application[];
    if (data.length === 0) {
      logger.info("No applications found");
      return 1;
    }
    const formattedData: Application[] = []
    for (const application of data) {
      formattedData.push({ "Name": application.Name, "ID": application.ID, "Version": application.Version, "DatabaseName": application.DatabaseName, "MaxVMs": application.MaxVMs, "Status": application.Status });
    }
    logger.info(`Listing applications for ${userCredentials.userName}`)
    console.log(JSON.stringify(formattedData));
    return 0;
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`Failed to list applications: ${e.response?.data}`);
      return 1;
    } else {
      logger.error(`Failed to list applications: ${(e as Error).message}`);
      return 1;
    }
  }
}
