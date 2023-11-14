import axios from "axios";
import { createGlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";

type Application = {
  Name: string;
  ID: string;
  Status: string;
};

export async function listApps(host: string, port: string) {
  const logger = createGlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    const list = await axios.get(
      `http://${host}:${port}/${userCredentials.userName}/application`,
      {
        headers: {
          Authorization: bearerToken,
        },
      }
    );
    const data: Application[] = list.data as Application[];
    if (data.length === 0) {
      logger.info("no application found");
      return;
    }
    for (const application of data) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
      console.log({ Name: application.Name, ID: application.ID, Status: application.Status });
    }
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to list applications: ${e.response?.data}`);
    } else {
      logger.error(`failed to list applications: ${(e as Error).message}`);
    }
  }
}
