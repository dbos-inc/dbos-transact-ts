import axios, { AxiosError } from "axios";
import { existsSync, readFileSync } from 'fs';
import { handleAPIErrors, dbosConfigFilePath, getCloudCredentials, getLogger, checkReadFile, sleep, isCloudAPIErrorResponse, retrieveApplicationName, dbosEnvPath, CloudAPIErrorResponse, CLILogger } from "../cloudutils.js";
import path from "path";
import { Application } from "./types.js";
import JSZip from "jszip";
import fg from 'fast-glob';
import chalk from "chalk";

type DeployOutput = {
  ApplicationName: string;
  ApplicationVersion: string;
}

function convertPathForGlob(p: string) {
  if (path.sep === '\\') {
      return p.replace(/\\/g, '/');
  }
  return p;
}
async function createZipData(logger: CLILogger): Promise<string> {
    const zip = new JSZip();

    const globPattern = convertPathForGlob(path.join(process.cwd(), '**', '*'));

    const files = await fg(globPattern, {
      dot: true,
      onlyFiles: true,
      ignore: [
        `**/${dbosEnvPath}/**`,
        '**/node_modules/**',
        '**/dist/**',
        '**/.git/**',
        `**/${dbosConfigFilePath}`,
      ]
    });

    files.forEach(file => {
        logger.debug(`    Zipping file ${file}`);
        const relativePath = path.relative(process.cwd(), file).replace(/\\/g, '/');
        const fileData = readFileSync(file);
        zip.file(relativePath, fileData, { binary: true });
    });

    // Add the interpolated config file at package root

    logger.debug(`    Interpreting configuration from ${dbosConfigFilePath}`);
    const interpolatedConfig = readInterpolatedConfig(dbosConfigFilePath, logger);
    zip.file(dbosConfigFilePath, interpolatedConfig, { binary: true });

    // Generate ZIP file as a Buffer
    logger.debug(`    Finalizing zip archive ...`);
    const buffer = await zip.generateAsync({ type: 'nodebuffer' });
    logger.debug(`    ... zip archive complete.`);
    return buffer.toString('base64');
}

export async function deployAppCode(host: string, rollback: boolean, previousVersion: string | null, verbose: boolean, targetDatabaseName: string | null = null): Promise<number> {
  const logger = getLogger(verbose);
  logger.debug("Getting cloud credentials...");
  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  logger.debug("  ... got cloud credentials");

  logger.debug("Retrieving app name...");
  const appName = retrieveApplicationName(logger);
  if (!appName) {
    logger.error("Failed to get app name.");
    return 1;
  }
  logger.debug(`  ... app name is ${appName}.`);

  // Verify that package-lock.json exists
  logger.debug("Checking for package-lock.json...");
  if (!existsSync(path.join(process.cwd(), 'package-lock.json'))) {
    logger.error("package-lock.json not found. Please run 'npm install' before deploying.")
    return 1;
  }
  logger.debug("  ... package-lock.json exists.");

  try {
    const body: {application_archive?: string, previous_version?: string, target_database_name?: string} = {}
    if (previousVersion === null) {
      logger.debug("Creating application zip ...");
      body.application_archive = await createZipData(logger);
      logger.debug("  ... application zipped.");
    } else {
      logger.info(`Restoring previous version ${previousVersion}`);
      body.previous_version = previousVersion
    }

    if (targetDatabaseName !== null) {
      logger.info(`Changing database instance for ${appName} to ${targetDatabaseName} and redeploying`)
      body.target_database_name = targetDatabaseName;
    }

    // Submit the deploy request
    let url = '';
    if (rollback) {
      url = `https://${host}/v1alpha1/${userCredentials.userName}/applications/${appName}/rollback`;
    } else if (targetDatabaseName !== null) {
      url = `https://${host}/v1alpha1/${userCredentials.userName}/applications/${appName}/changedbinstance`;
    } else {
      url = `https://${host}/v1alpha1/${userCredentials.userName}/applications/${appName}`;
    }

    logger.info(`Submitting deploy request for ${appName}`)
    const response = await axios.post(
      url,
      body,
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
        console.log(chalk.red("Did you register this application? Hint: run `npx dbos-cloud app register -d <database-instance-name>` to register your app and try again"));
      }
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

function readInterpolatedConfig(configFilePath: string, logger: CLILogger): string {
  const configFileContent = checkReadFile(configFilePath) as string;
  const regex = /\${([^}]+)}/g;  // Regex to match ${VAR_NAME} style placeholders
  return configFileContent.replace(regex, (_, g1: string) => {
    if (process.env[g1] !== undefined) {
      logger.debug(`      Substituting value of '${g1}' from process environment.`);
      return process.env[g1] ?? "";
    }
    if (g1 !== "PGPASSWORD") {
      logger.warn(`      Variable '${g1}' would be substituted from the process environment, but is not defined.`);
    }
    return "";
  });
}
