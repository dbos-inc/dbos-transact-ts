import axios from "axios";
import YAML from "yaml";
import { execSync } from "child_process";
import fs from "fs";
import FormData from "form-data";
import { createGlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";
import { createDirectory } from "../../utils";
import { ConfigFile, loadConfigFile, operonConfigFilePath } from "../../operon-runtime/config";

const deployDirectoryName = "operon_deploy";

export async function deployAppCode(appName: string, host: string, port: string) {
  const logger = createGlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    createDirectory(deployDirectoryName);

    const configFile: ConfigFile | undefined = loadConfigFile(operonConfigFilePath);
    if (!configFile) {
      logger.error(`failed to parse ${operonConfigFilePath}`);
      return;
    }

    // Parse version from config file. If missing, use current epoch
    const version: string = configFile.version ?? Date.now().toString();
    configFile.version = version;

    try {
      fs.writeFileSync(`${deployDirectoryName}/${operonConfigFilePath}`, YAML.stringify(configFile));
    } catch (e) {
      logger.error(`failed to write ${operonConfigFilePath}: ${(e as Error).message}`);
      return;
    }

    execSync(`zip -ry ${deployDirectoryName}/${appName}.zip ./* -x ${deployDirectoryName}/* ${operonConfigFilePath} > /dev/null`);
    execSync(`zip -j ${deployDirectoryName}/${appName}.zip ${deployDirectoryName}/${operonConfigFilePath} > /dev/null`);

    const formData = new FormData();
    formData.append("app_archive", fs.createReadStream(`${deployDirectoryName}/${appName}.zip`));
    formData.append("application_version", version);

    await axios.post(`http://${host}:${port}/${userCredentials.userName}/application/${appName}`, formData, {
      headers: {
        ...formData.getHeaders(),
        Authorization: bearerToken,
      },
    });
    logger.info(`Successfully deployed ${appName} with version ${version}`);
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to deploy application ${appName}: ${e.response?.data}`);
    } else {
      logger.error(`failed to deploy application ${appName}: ${(e as Error).message}`);
    }
  }
}
