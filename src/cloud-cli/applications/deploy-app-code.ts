import axios from "axios";
import { execSync } from "child_process";
import fs from "fs";
import FormData from "form-data";
import { createGlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";

export async function deployAppCode(appName: string, host: string, port: string) {
  const logger = createGlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    execSync(`mkdir -p operon_deploy`);
    execSync(`envsubst < operon-config.yaml > operon_deploy/operon-config.yaml`);
    execSync(`zip -ry operon_deploy/${appName}.zip ./* -x operon_deploy/* operon-config.yaml > /dev/null`);
    execSync(`zip -j operon_deploy/${appName}.zip operon_deploy/operon-config.yaml > /dev/null`);

    const formData = new FormData();
    formData.append("app_archive", fs.createReadStream(`operon_deploy/${appName}.zip`));

    await axios.post(`http://${host}:${port}/${userCredentials.userName}/application/${appName}`, formData, {
      headers: {
        ...formData.getHeaders(),
        Authorization: bearerToken,
      },
    });
    logger.info(`Successfully deployed: ${appName}`);
    logger.info(`${appName} ID: ${appName}`);
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to deploy application ${appName}: ${e.response?.data}`);
    } else {
      logger.error(`failed to deploy application ${appName}: ${(e as Error).message}`);
    }
  }
}
