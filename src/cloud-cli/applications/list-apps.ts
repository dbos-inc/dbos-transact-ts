import axios from "axios";
import { GlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";
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
      logger.info("no application found");
      return 1;
    }
    const formattedData: Application[] = []
    for (const application of data) {
      formattedData.push({ "Name": application.Name, "ID": application.ID, "Version": application.Version, "DatabaseName": application.DatabaseName, "MaxVMs": application.MaxVMs });
    }
    console.log(JSON.stringify(formattedData));
    return 0;
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to list applications: ${e.response?.data}`);
      return 1;
    } else {
      logger.error(`failed to list applications: ${(e as Error).message}`);
      return 1;
    }
  }
}
