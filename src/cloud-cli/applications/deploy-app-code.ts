import axios from "axios";
import YAML from "yaml";
import { execSync } from "child_process";
import fs from "fs";
import { GlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";
import { createDirectory, readFileSync } from "../../utils";
import { ConfigFile, loadConfigFile, dbosConfigFilePath } from "../../dbos-runtime/config";

const deployDirectoryName = "dbos_deploy";

export async function deployAppCode(appName: string, host: string, port: string): Promise<number> {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    createDirectory(deployDirectoryName);

    const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
    if (!configFile) {
      logger.error(`failed to parse ${dbosConfigFilePath}`);
      return 1;
    }

    // Parse version from config file. If missing, use current epoch
    const version: string = configFile.version ?? Date.now().toString();
    configFile.version = version;

    // Inject OTel export configuration
    if (!configFile.telemetry) {
      configFile.telemetry = {};
    }
    configFile.telemetry.OTLPExporter = {
      logsEndpoint: "http://otel-collector:4318/v1/logs",
      tracesEndpoint: "http://otel-collector:4318/v1/traces",
    };

    try {
      fs.writeFileSync(`${deployDirectoryName}/${dbosConfigFilePath}`, YAML.stringify(configFile));
    } catch (e) {
      logger.error(`failed to write ${dbosConfigFilePath}: ${(e as Error).message}`);
      return 1;
    }

    execSync(`npm prune --omit=dev node_modules/* `);
    execSync(`zip -ry ${deployDirectoryName}/${appName}.zip ./* -x ${deployDirectoryName}/* ${dbosConfigFilePath} > /dev/null`);
    execSync(`zip -j ${deployDirectoryName}/${appName}.zip ${deployDirectoryName}/${dbosConfigFilePath} > /dev/null`);

    const zipData = readFileSync(`${deployDirectoryName}/${appName}.zip`, "base64");
    await axios.post(
      `https://${host}:${port}/${userCredentials.userName}/application/${appName}`,
      {
        application_version: version,
        application_archive: zipData,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );
    logger.info(`Successfully deployed ${appName} with version ${version}`);
    return 0;
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to deploy application ${appName}: ${e.response?.data}`);
      return 1;
    } else {
      logger.error(`failed to deploy application ${appName}: ${(e as Error).message}`);
      return 1;
    }
  }
}
