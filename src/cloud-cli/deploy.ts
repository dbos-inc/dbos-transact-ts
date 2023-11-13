import axios from "axios";
import YAML from "yaml";
import { execSync } from "child_process";
import fs from "fs";
import FormData from "form-data";
import { createGlobalLogger } from "../telemetry/logs";
import { getCloudCredentials } from "./utils";
import { ConfigFile, parseConfigFile, operonConfigFilePath } from "../operon-runtime/config";

export async function deploy(appName: string, host: string, port: string, machines: number) {
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
    execSync(`mkdir -p operon_deploy`);

    const configFile: ConfigFile | undefined = parseConfigFile(operonConfigFilePath);
    if (!configFile) {
      logger.error(`failed to parse ${operonConfigFilePath}`);
      return;
    }
    try {
      fs.writeFileSync(`operon_deploy/${operonConfigFilePath}`, YAML.stringify(configFile));
    } catch (e) {
      logger.error(`failed to write ${operonConfigFilePath}: ${(e as Error).message}`);
      return;
    }

    execSync(`zip -ry operon_deploy/${uuid}.zip ./* -x operon_deploy/* ${operonConfigFilePath} > /dev/null`);
    execSync(`zip -j operon_deploy/${uuid}.zip operon_deploy/${operonConfigFilePath} > /dev/null`);

    const formData = new FormData();
    formData.append("app_archive", fs.createReadStream(`operon_deploy/${uuid}.zip`));
    formData.append("application_version", configFile.version);

    await axios.post(`http://${host}:${port}/${userCredentials.userName}/application/${appName}`, formData, {
      headers: {
        ...formData.getHeaders(),
        Authorization: bearerToken,
      },
    });
    logger.info(`Successfully deployed: ${appName}`);
    logger.info(`${appName} ID: ${uuid}`);
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to deploy application ${appName}: ${e.response?.data}`);
    } else {
      logger.error(`failed to deploy application ${appName}: ${(e as Error).message}`);
    }
  }
}
