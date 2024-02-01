import axios, { AxiosError } from "axios";
import { execSync } from "child_process";
import { writeFileSync, existsSync } from 'fs';
import { handleAPIErrors, createDirectory, dbosConfigFilePath, getCloudCredentials, getLogger, readFileSync, runCommand, sleep } from "../cloudutils";
import path from "path";
import { Application } from "./types";

const deployDirectoryName = "dbos_deploy";

type DeployOutput = {
  ApplicationName: string;
  ApplicationVersion: string;
}

export async function deployAppCode(host: string, docker: boolean): Promise<number> {
  const startTime = process.hrtime();
  const logger = getLogger()
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
  const appName = packageJson.name;
  logger.info(`Loaded application name from package.json: ${appName}`)

  createDirectory(deployDirectoryName);

  // Verify that package-lock.json exists
  if (!existsSync(path.join(process.cwd(), 'package-lock.json'))) {
    logger.error("package-lock.json not found. Please run 'npm install' before deploying.")
    return 1;
  }

  if (docker) {
    // Build the application inside a Docker container using the same base image as our cloud setup
    logger.info(`Building ${appName} using Docker`)
    const dockerSuccess = await buildAppInDocker(appName);
    if (!dockerSuccess) {
      return 1;
    }
  } else {
    // Zip the current directory and deploy from there. Requires app to have already been built. Only for testing.
    execSync(`zip -ry ${deployDirectoryName}/${appName}.zip ./* -x ${deployDirectoryName}/* > /dev/null`);
  }

  const interpolatedConfig = readInterpolatedConfig(dbosConfigFilePath)
  writeFileSync(`${deployDirectoryName}/${dbosConfigFilePath}`, interpolatedConfig)
  execSync(`zip -j ${deployDirectoryName}/${appName}.zip ${deployDirectoryName}/${dbosConfigFilePath} > /dev/null`);

  try {
    const zipData = readFileSync(`${deployDirectoryName}/${appName}.zip`, "base64");

    // Submit the deploy request
    logger.info(`Submitting deploy request for ${appName}`)
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
    const deployEndTime = process.hrtime(startTime);
    const deployLatency = (deployEndTime[0] * 1000) + (deployEndTime[1] / 1000000);
    logger.info(`Application ${appName} code deployed in ${deployLatency.toFixed(3)} ms`)
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
    const endTime = process.hrtime(startTime);
    const elapsed = (endTime[0] * 1000) + (endTime[1] / 1000000);
    logger.info(`Application ${appName} successfuly deployed in ${elapsed.toFixed(3)} ms`)
    logger.info(`Access your application at https://${host}/${userCredentials.userName}/application/${appName}`)
    return 0;
  } catch (e) {
    const errorLabel = `Failed to deploy application ${appName}`;
    if (axios.isAxiosError(e) && (e as AxiosError).response) {
      handleAPIErrors(errorLabel, e);
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

async function buildAppInDocker(appName: string): Promise<boolean> {
  const logger = getLogger();

  // Verify Docker is running
  try {
    execSync(`docker > /dev/null 2>&1`)
  } catch (e) {
    logger.error("Docker not available.  To deploy, please start the Docker daemon and make the `docker` command runnable without sudo.")
    return false
  }

  const dockerFileName = `${deployDirectoryName}/Dockerfile.dbos`;
  const containerName = `dbos-builder-${appName}`;

  // Dockerfile content
  const dockerFileContent = `
FROM node:lts-bookworm-slim
RUN apt update
RUN apt install -y zip
WORKDIR /app
COPY . .
RUN npm clean-install
RUN npm run build
RUN npm prune --omit=dev
RUN zip -ry ${appName}.zip ./* -x "${appName}.zip" -x "${deployDirectoryName}/*" > /dev/null
`;
  const dockerIgnoreContent = `
node_modules/
${deployDirectoryName}/
dist/
`;
  try {
    // Write the Dockerfile and .dockerignore
    writeFileSync(dockerFileName, dockerFileContent);
    writeFileSync(`${deployDirectoryName}/Dockerfile.dbos.dockerignore`, dockerIgnoreContent);
    // Build the Docker image.  As build takes a long time, use runCommand to stream its output to stdout.
    await runCommand('docker', ['build', '-t', appName, '-f', dockerFileName, '.'])
    // Run the container
    execSync(`docker run -d --name ${containerName} ${appName}`);
    // Copy the archive from the container to the local deploy directory
    execSync(`docker cp ${containerName}:/app/${appName}.zip ${deployDirectoryName}/${appName}.zip`);
    // Stop and remove the container
    execSync(`docker stop ${containerName}`);
    execSync(`docker rm ${containerName}`);
    return true;
  } catch (e) {
    logger.error(`Failed to build application ${appName}: ${(e as Error).message}`);
    return false;
  }
}
