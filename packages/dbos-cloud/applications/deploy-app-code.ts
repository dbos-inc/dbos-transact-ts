import axios, { AxiosError } from "axios";
import { existsSync, readFileSync } from 'fs';
import { handleAPIErrors, dbosConfigFilePath, getCloudCredentials, getLogger, checkReadFile, sleep, isCloudAPIErrorResponse, retrieveApplicationName, dbosEnvPath, CloudAPIErrorResponse } from "../cloudutils.js";
import path from "path";
import { Application } from "./types.js";
import JSZip from "jszip";
import fg from 'fast-glob';

type DeployOutput = {
  ApplicationName: string;
  ApplicationVersion: string;
}

async function createZipData(): Promise<string> {
    const zip = new JSZip();

    const files = await fg(`${process.cwd()}/**/*`, { dot: true, onlyFiles: true, ignore: [`**/${dbosEnvPath}/**`, '**/node_modules/**', '**/dist/**', `**/${dbosConfigFilePath}`] });

    files.forEach(file => {
        const relativePath = file.replace(`${process.cwd()}/`, '');
        const fileData = readFileSync(file);
        zip.file(relativePath, fileData, { binary: true });
    });

    // Add the interpolated config file at package root
    const interpolatedConfig = readInterpolatedConfig(dbosConfigFilePath)
    zip.file(dbosConfigFilePath, interpolatedConfig, { binary: true });

    // Generate ZIP file as a Buffer
    const buffer = await zip.generateAsync({ type: 'nodebuffer' });
    return buffer.toString('base64');
}


export async function deployAppCode(host: string, rollback: boolean): Promise<number> {
  const logger = getLogger()
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  const appName = retrieveApplicationName(logger);
  if (!appName) {
    return 1;
  }

  // Verify that package-lock.json exists
  if (!existsSync(path.join(process.cwd(), 'package-lock.json'))) {
    logger.error("package-lock.json not found. Please run 'npm install' before deploying.")
    return 1;
  }

  try {
    const zipData = await createZipData();

    // Submit the deploy request
    let url = '';
    if (rollback) {
      url = `https://${host}/v1alpha1/${userCredentials.userName}/applications/${appName}/rollback`;
    } else {
      url = `https://${host}/v1alpha1/${userCredentials.userName}/applications/${appName}`;
    }

    logger.info(`Submitting deploy request for ${appName}`)
    const response = await axios.post(
      url,
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
    logger.info(`Access your application at https://${userCredentials.userName}-${appName}.${host}/`)
    return 0;
  } catch (e) {
    const errorLabel = `Failed to deploy application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
      const resp: CloudAPIErrorResponse = axiosError.response?.data;
      if (resp.message.includes(`application ${appName} not found`)) {
        console.error('\x1b[31m%s\x1b[0m', "Did you register this application? Hint: run `npx dbos-cloud app register -d <database-instance-name>` to register your app and try again");
      }
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

function readInterpolatedConfig(configFilePath: string): string {
  const configFileContent = checkReadFile(configFilePath) as string;
  const regex = /\${([^}]+)}/g;  // Regex to match ${VAR_NAME} style placeholders
  return configFileContent.replace(regex, (_, g1: string) => {
    return process.env[g1] || "";  // If the env variable is not set, return an empty string.
});
}
