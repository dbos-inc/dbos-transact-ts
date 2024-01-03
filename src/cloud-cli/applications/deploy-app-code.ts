import axios from "axios";
import { execSync } from "child_process";
import { writeFileSync } from 'fs';
import { GlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";
import { createDirectory, readFileSync, sleep } from "../../utils";
import path from "path";
import { Application } from "./types";



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
  logger.info(`Submitting deploy request for ${appName}`)

  try {
    createDirectory(deployDirectoryName);

    buildAppInDocker(appName);

    const zipData = readFileSync(`${deployDirectoryName}/${appName}.zip`, "base64");

    // Submit the deploy request
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
    logger.info(`Submitted deploy request for ${appName}. Assigned version: ${deployOutput.ApplicationVersion}`);

    // Wait for the application to become available
    let count = 0
    let applicationAvailable = false
    while (!applicationAvailable) {
      count += 1
      if (count % 5 === 0) {
        logger.info(`Waiting for ${appName} with version ${deployOutput.ApplicationVersion} to be available`);
        if (count > 20) {
          logger.info(`If ${appName} takes too long to become available, check its logs at...`);
        }
      }

      // Retrieve the application status, check if it is "AVAILABLE"
      const list = await axios.get(
        `https://${host}/${userCredentials.userName}/application`,
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
      await sleep(1000)
    }
    logger.info(`Application ${appName} successfuly deployed`)
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

function buildAppInDocker(appName: string) {
    const dockerFileName = `${deployDirectoryName}/Dockerfile.dbos`;
    const containerName = 'dbos-builder';

    // Dockerfile content
    const dockerFileContent = `
FROM node:lts-bookworm-slim
RUN apt update
RUN apt install -y zip
WORKDIR /app
COPY . .
RUN npm run build
RUN npm prune --omit=dev
RUN zip -ry ${appName}.zip ./* -x "${appName}.zip" -x "${deployDirectoryName}/*" > /dev/null
`;

    // Write the Dockerfile
    writeFileSync(dockerFileName, dockerFileContent);
    // Build the Docker image
    execSync(`docker build -t ${appName} -f ${dockerFileName} .`);
    // Run the container
    execSync(`docker run -d --name ${containerName} ${appName}`);
    // Copy the archive from the container to the local deploy directory
    execSync(`docker cp ${containerName}:/app/${appName}.zip ${deployDirectoryName}/${appName}.zip`);
    // Stop and remove the container
    execSync(`docker stop ${containerName}`);
    execSync(`docker rm ${containerName}`);
}
