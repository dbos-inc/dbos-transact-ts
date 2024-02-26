import axios, { AxiosError } from "axios";
import { execSync } from "child_process";
import { writeFileSync, existsSync } from 'fs';
import { handleAPIErrors, createDirectory, dbosConfigFilePath, getCloudCredentials, getLogger, readFileSync, runCommand, sleep, isCloudAPIErrorResponse, retrieveApplicationName } from "../cloudutils";
import path from "path";
import { Application } from "./types";

const deployDirectoryName = "dbos_deploy";

type DeployOutput = {
  ApplicationName: string;
  ApplicationVersion: string;
}

export async function deployAppCode(host: string, docker: boolean): Promise<number> {
  const logger = getLogger()
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  const appName = retrieveApplicationName(logger);
  if (appName === null) {
    return 1;
  }
  logger.info(`Loaded application name from package.json: ${appName}`)

  createDirectory(deployDirectoryName);

  // Verify that package-lock.json exists
  if (!existsSync(path.join(process.cwd(), 'package-lock.json'))) {
    logger.error("package-lock.json not found. Please run 'npm install' before deploying.")
    return 1;
  }

  execSync(`zip -ry ${deployDirectoryName}/${appName}.zip ./* -x ${deployDirectoryName}/* -x node_modules/* -x dist/* > /dev/null`);
  const interpolatedConfig = readInterpolatedConfig(dbosConfigFilePath)
  writeFileSync(`${deployDirectoryName}/${dbosConfigFilePath}`, interpolatedConfig)
  execSync(`zip -j ${deployDirectoryName}/${appName}.zip ${deployDirectoryName}/${dbosConfigFilePath} > /dev/null`);

  try {
    const zipData = readFileSync(`${deployDirectoryName}/${appName}.zip`, "base64");

    // Submit the deploy request
    logger.info(`Submitting deploy request for ${appName}`)
    const response = await axios.post(
      `https://${host}/v1alpha1/${userCredentials.userName}/applications/${appName}`,
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
    logger.info(`Submitted deploy request for ${appName}. Assigned version: ${deployOutput.ApplicationVersion}`);

    // Wait for the application to become available
    let count = 0
    let applicationAvailable = false
    while (!applicationAvailable) {
      count += 1
      if (count % 5 === 0) {
        logger.info(`Waiting for ${appName} with version ${deployOutput.ApplicationVersion} to be available`);
        if (count > 20) {
          logger.info(`If ${appName} takes too long to become available, check its logs with 'npx dbos-cloud applications logs'`);
        }
      }
      if (count > 180) {
        logger.error("Application taking too long to become available")
        return 1;
      }

      // Retrieve the application status, check if it is "AVAILABLE"
      const list = await axios.get(
        `https://${host}/v1alpha1/${userCredentials.userName}/applications`,
        {
          headers: {
            Authorization: bearerToken,
          },
        }
      );
      const applications: Application[] = list.data as Application[];
      for (const application of applications) {
        if (application.Name === appName && application.Status === "AVAILABLE") {
          applicationAvailable = true
        }
      }
      await sleep(1000);
    }
    await sleep(5000); // Leave time for route cache updates
    logger.info(`Successfully deployed ${appName}!`)
    logger.info(`Access your application at https://${host}/apps/${userCredentials.userName}/${appName}`)
    return 0;
  } catch (e) {
    const errorLabel = `Failed to deploy application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

function readInterpolatedConfig(configFilePath: string): string {
  const configFileContent = readFileSync(configFilePath) as string;
  const regex = /\${([^}]+)}/g;  // Regex to match ${VAR_NAME} style placeholders
  return configFileContent.replace(regex, (_, g1: string) => {
    return process.env[g1] || "";  // If the env variable is not set, return an empty string.
});
}
