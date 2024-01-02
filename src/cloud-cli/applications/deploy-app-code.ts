import axios from "axios";
import YAML from "yaml";
import { execSync } from "child_process";
import fs from "fs";
import { GlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";
import { createDirectory, readFileSync } from "../../utils";
import { ConfigFile, loadConfigFile, dbosConfigFilePath } from "../../dbos-runtime/config";
import path from "path";

const deployDirectoryName = "dbos_deploy";

type DeployOutput = {
  ApplicationName: string;
  ApplicationVersion: string;
}

export async function deployAppCode(host: string): Promise<number> {
  const logger = new GlobalLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  logger.info(`Loaded application name from package.json: ${appName}`)
  logger.info(`Deploying application: ${appName}`)

  try {
    createDirectory(deployDirectoryName);

    execSync(`npm prune --omit=dev node_modules/* `);
    execSync(`zip -ry ${deployDirectoryName}/${appName}.zip ./* -x ${deployDirectoryName}/* > /dev/null`);

    const zipData = readFileSync(`${deployDirectoryName}/${appName}.zip`, "base64");
    const response = await axios.post(
      `https://${host}/${userCredentials.userName}/application/${appName}`,
      {
        application_archive: zipData,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );
    const deployOutput = response.data as DeployOutput;
    logger.info(`Successfully deployed ${appName} with version ${deployOutput.ApplicationVersion}`);
    logger.info(`Access your application at https://${host}/${userCredentials.userName}/application/${appName}`)
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
