import axios, { AxiosError } from "axios";
import { statSync, existsSync, readFileSync } from "fs";
import {
  handleAPIErrors,
  dbosConfigFilePath,
  getCloudCredentials,
  getLogger,
  checkReadFile,
  sleepms,
  isCloudAPIErrorResponse,
  retrieveApplicationName,
  dbosEnvPath,
  CloudAPIErrorResponse,
  CLILogger,
  retrieveApplicationLanguage,
  AppLanguages,
  DBOSCloudCredentials,
} from "../cloudutils.js";
import path from "path";
import { Application } from "./types.js";
import JSZip from "jszip";
import fg from "fast-glob";
import chalk from "chalk";
import { createUserDb, UserDBInstance } from "../databases/databases.js";
import { registerApp } from "./register-app.js";
import { input, select } from "@inquirer/prompts";
import { Logger } from "winston";

type DeployOutput = {
  ApplicationName: string;
  ApplicationVersion: string;
};

function convertPathForGlob(p: string) {
  if (path.sep === "\\") {
    return p.replace(/\\/g, "/");
  }
  return p;
}

function getFilePermissions(filePath: string) {
  const stats = statSync(filePath);
  return stats.mode;
}

async function createZipData(logger: CLILogger): Promise<string> {
  const zip = new JSZip();

  const globPattern = convertPathForGlob(path.join(process.cwd(), "**", "*"));

  const files = await fg(globPattern, {
    dot: true,
    onlyFiles: true,
    ignore: [`**/${dbosEnvPath}/**`, "**/node_modules/**", "**/dist/**", "**/.git/**", `**/${dbosConfigFilePath}`],
  });

  files.forEach((file) => {
    logger.debug(`    Zipping file ${file}`);
    const relativePath = path.relative(process.cwd(), file).replace(/\\/g, "/");
    const fileData = readFileSync(file);
    const filePerms = getFilePermissions(file);
    logger.debug(`      File permissions: ${filePerms.toString(8)}`);
    zip.file(relativePath, fileData, { binary: true, unixPermissions: filePerms});
  });

  // Add the interpolated config file at package root

  logger.debug(`    Interpreting configuration from ${dbosConfigFilePath}`);
  const interpolatedConfig = readInterpolatedConfig(dbosConfigFilePath, logger);
  zip.file(dbosConfigFilePath, interpolatedConfig, { binary: true });

  // Generate ZIP file as a Buffer
  logger.debug(`    Finalizing zip archive ...`);
  const buffer = await zip.generateAsync({ platform: "UNIX", type: "nodebuffer" });
  logger.debug(`    ... zip archive complete (${buffer.length} bytes).`);
  return buffer.toString("base64");
}

export async function deployAppCode(
  host: string,
  rollback: boolean,
  previousVersion: string | null,
  verbose: boolean,
  targetDatabaseName: string | null = null,
  appName: string | undefined,
  userDBName: string | undefined = undefined,
  enableTimeTravel: boolean = false
): Promise<number> {
  const logger = getLogger(verbose);
  logger.debug("Getting cloud credentials...");
  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  logger.debug("  ... got cloud credentials");

  logger.debug("Retrieving app name...");
  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    logger.error("Failed to get app name.");
    return 1;
  }
  logger.debug(`  ... app name is ${appName}.`);

  const appLanguage = retrieveApplicationLanguage();

  if (appLanguage === AppLanguages.Node as string) {
    logger.debug("Checking for package-lock.json...");
    const packageLockJsonExists = existsSync(path.join(process.cwd(), "package-lock.json"));
    logger.debug(`  ... package-lock.json found: ${packageLockJsonExists}`);

    if (!packageLockJsonExists) {
      logger.error("No package-lock.json found. Please run 'npm install' before deploying.");
      return 1;
    }
  } else if (appLanguage === AppLanguages.Python as string) {
    logger.debug("Checking for requirements.txt...");
    const requirementsTxtExists = existsSync(path.join(process.cwd(), "requirements.txt"));
    logger.debug(`  ... requirements.txt found: ${requirementsTxtExists}`);

    if (!requirementsTxtExists) {
      logger.error("No requirements.txt found. Please create one before deploying.");
      return 1;
    }
  } else {
    logger.error(`dbos-config.yaml contains invalid language ${appLanguage}`)
    return 1;
  }

  // First, check if the application exists
  const appRegistered = await isAppRegistered(logger, host, appName, userCredentials);

  // If the app is not registered, register it.
  if (!appRegistered) {
    userDBName = await chooseAppDBServer(logger, host, userCredentials, userDBName);
    if (userDBName === "") {
      return 1;
    }
    // Register the app
    if (enableTimeTravel) {
      logger.info("Enabling time travel for this application");
    } else {
      logger.info("Time travel is disabled for this application");
    }
    await registerApp(userDBName, host, enableTimeTravel, appName);

  }

  try {
    const body: { application_archive?: string; previous_version?: string; target_database_name?: string } = {};
    if (previousVersion === null) {
      logger.debug("Creating application zip ...");
      body.application_archive = await createZipData(logger);
      logger.debug("  ... application zipped.");
    } else {
      logger.info(`Restoring previous version ${previousVersion}`);
      body.previous_version = previousVersion;
    }

    if (targetDatabaseName !== null) {
      logger.info(`Changing database instance for ${appName} to ${targetDatabaseName} and redeploying`);
      body.target_database_name = targetDatabaseName;
    }

    // Submit the deploy request
    let url = "";
    if (rollback) {
      url = `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}/rollback`;
    } else if (targetDatabaseName !== null) {
      url = `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}/changedbinstance`;
    } else {
      url = `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}`;
    }

    logger.info(`Submitting deploy request for ${appName}`);
    const response = await axios.post(url, body, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    const deployOutput = response.data as DeployOutput;
    logger.info(`Submitted deploy request for ${appName}. Assigned version: ${deployOutput.ApplicationVersion}`);

    // Wait for the application to become available
    let count = 0;
    let applicationAvailable = false;
    while (!applicationAvailable) {
      count += 1;
      if (count % 5 === 0) {
        logger.info(`Waiting for ${appName} with version ${deployOutput.ApplicationVersion} to be available`);
        if (count > 20) {
          logger.info(`If ${appName} takes too long to become available, check its logs with 'npx dbos-cloud applications logs'`);
        }
      }
      if (count > 180) {
        logger.error("Application taking too long to become available");
        return 1;
      }

      // Retrieve the application status, check if it is "AVAILABLE"
      const list = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/applications`, {
        headers: {
          Authorization: bearerToken,
        },
      });
      const applications: Application[] = list.data as Application[];
      for (const application of applications) {
        if (application.Name === appName && application.Status === "AVAILABLE") {
          applicationAvailable = true;
        }
      }
      await sleepms(1000);
    }
    await sleepms(5000); // Leave time for route cache updates
    logger.info(`Successfully deployed ${appName}!`);
    logger.info(`Access your application at https://${userCredentials.organization}-${appName}.${host}/`);
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
  const regex = /\${([^}]+)}/g; // Regex to match ${VAR_NAME} style placeholders
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

async function isAppRegistered(logger: Logger, host: string, appName: string, userCredentials: DBOSCloudCredentials): Promise<boolean> {
  let appRegistered = true;
  const bearerToken = "Bearer " + userCredentials.token;
  try {
    await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
  } catch (e) {
    const errorLabel = `Failed to retrieve info for application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      const resp: CloudAPIErrorResponse = axiosError.response?.data;
      if (resp.message.includes(`application ${appName} not found`)) {
        appRegistered = false;
      } else {
        handleAPIErrors(errorLabel, axiosError);
      }
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
  }
  return appRegistered;
}

async function chooseAppDBServer(logger: Logger, host: string, userCredentials: DBOSCloudCredentials, userDBName: string = ""): Promise<string> {
  // List existing database instances.
  let userDBs: UserDBInstance[] = [];
  const bearerToken = "Bearer " + userCredentials.token;
  try {
    const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/databases`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    userDBs = res.data as UserDBInstance[];
  } catch (e) {
    const errorLabel = `Failed to list databases`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return "";
  }

  if (userDBName) {
    // Check if the database instance exists or not.
    const dbExists = userDBs.some((db) => db.PostgresInstanceName === userDBName);
    if (dbExists) {
      return userDBName;
    }
  }

  if (userDBName || userDBs.length === 0) {
    // If not, prompt the user to provision one.
    if (!userDBName) {
      logger.info("No database found, provisioning a database instance (server)...");
      userDBName = await input({
        message: "Database instance name?",
        default: `${userCredentials.userName}-db-server`,
      });
    } else {
      logger.info(`Database instance ${userDBName} not found, provisioning a new one...`);
    }

    // Use a default user name and auto generated password.
    const appDBUserName = "dbos_user";
    const appDBPassword = Buffer.from(Math.random().toString()).toString("base64");
    const res = await createUserDb(host, userDBName, appDBUserName, appDBPassword, true);
    if (res !== 0) {
      return "";
    }
  } else if (userDBs.length > 1) {
    // If there is more than one database instances, prompt the user to select one.
    userDBName = await select({
      message: "Choose a database instance for this app:",
      choices: userDBs.map((db) => ({
        name: db.PostgresInstanceName,
        value: db.PostgresInstanceName,
      })),
    });
  } else {
    // Use the only available database server.
    userDBName = userDBs[0].PostgresInstanceName;
    logger.info(`Using database instance: ${userDBName}`);
  }
  return userDBName;
}