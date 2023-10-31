import axios from "axios";
import fs from "fs";
import { createGlobalLogger } from "../telemetry/logs";
import { OperonCloudCredentials, operonEnvPath } from "./login";

export async function getAppLogs(appName: string, host: string) {
  const logger = createGlobalLogger();

  const userCredentials = JSON.parse(fs.readFileSync(`./${operonEnvPath}/credentials`).toString("utf-8")) as OperonCloudCredentials;
  const userName = userCredentials.userName;
  const userToken = userCredentials.token.replace(/\r|\n/g, ""); // Trim the trailing /r /n.
  const bearerToken = "Bearer " + userToken;

  try {
    const res = await axios.get(`http://${host}:8080/${userName}/logs/application/${appName}`, {
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
