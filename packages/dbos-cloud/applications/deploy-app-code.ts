import axios, { AxiosError } from 'axios';
import { statSync, existsSync, readFileSync } from 'fs';
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
} from '../cloudutils.js';
import path from 'path';
import { Application } from './types.js';
import JSZip from 'jszip';
import fg from 'fast-glob';
import chalk from 'chalk';
import { registerApp } from './register-app.js';
import { Logger } from 'winston';
import { chooseAppDBServer } from '../databases/databases.js';
import YAML from 'yaml';
import { ConfigFile } from '../configutils.js';
import fs from 'fs';

type DeployOutput = {
  ApplicationName: string;
  ApplicationVersion: string;
};

function convertPathForGlob(p: string) {
  if (path.sep === '\\') {
    return p.replace(/\\/g, '/');
  }
  return p;
}

function getFilePermissions(filePath: string) {
  const stats = statSync(filePath);
  return stats.mode;
}

function parseIgnoreFile(filePath: string) {
  if (!fs.existsSync(filePath)) return [];
  return fs
    .readFileSync(filePath, 'utf-8')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#')); // Exclude empty lines and comments
}

async function createZipData(logger: CLILogger): Promise<string> {
  const zip = new JSZip();

  const globPattern = convertPathForGlob(path.join(process.cwd(), '**', '*'));

  const dbosIgnoreFilePath = '.dbosignore';

  const ignorePatterns = parseIgnoreFile(dbosIgnoreFilePath);
  const globIgnorePatterns = ignorePatterns.map((pattern) => {
    pattern = convertPathForGlob(path.join(process.cwd(), pattern));
    if (pattern.endsWith('/')) {
      pattern = path.join(pattern, '**'); // Recursively ignore directories
    }
    return pattern;
  });
  const hardcodedIgnorePatterns = [
    `**/${dbosEnvPath}/**`,
    '**/node_modules/**',
    '**/dist/**',
    '**/.git/**',
    `**/${dbosConfigFilePath}`,
    '**/venv/**',
    '**/.venv/**',
    '**/.python-version',
  ];

  const files = await fg(globPattern, {
    dot: true,
    onlyFiles: true,
    ignore: [...hardcodedIgnorePatterns, ...globIgnorePatterns],
  });

  files.forEach((file) => {
    logger.debug(`    Zipping file ${file}`);
    const relativePath = path.relative(process.cwd(), file).replace(/\\/g, '/');
    const fileData = readFileSync(file);
    const filePerms = getFilePermissions(file);
    logger.debug(`      File permissions: ${filePerms.toString(8)}`);
    zip.file(relativePath, fileData, { binary: true, unixPermissions: filePerms });
  });

  // Add the interpolated config file at package root

  logger.debug(`    Interpreting configuration from ${dbosConfigFilePath}`);
  const interpolatedConfig = readInterpolatedConfig(dbosConfigFilePath, logger);
  zip.file(dbosConfigFilePath, interpolatedConfig, { binary: true });

  // Generate ZIP file as a Buffer
  logger.debug(`    Finalizing zip archive ...`);
  const buffer = await zip.generateAsync({ platform: 'UNIX', type: 'nodebuffer', compression: 'DEFLATE' });
  // Max string size is about 512MB. See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/length#description.
  if (buffer.length > 0x1fffffe8) {
    throw new Error(`Zip archive is too large (${buffer.length} bytes)`);
  }
  logger.debug(`    ... zip archive complete (${buffer.length} bytes).`);
  // This line could still fail if the buffer is too large. That's because base64 adds about 33% in size.
  return buffer.toString('base64');
}

export async function deployAppCode(
  host: string,
  rollback: boolean,
  previousVersion: string | null,
  verbose: boolean,
  targetDatabaseName: string | null = null, // Used for changing database instance
  appName: string | undefined,
  userDBName: string | undefined = undefined, // Used for registering the app
  enableTimeTravel: boolean = false,
): Promise<number> {
  const startTime = Date.now();
  const logger = getLogger(verbose);
  logger.debug('Getting cloud credentials...');
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;
  logger.debug('  ... got cloud credentials');

  logger.debug('Retrieving app name...');
  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    logger.error('Failed to get app name.');
    return 1;
  }
  logger.debug(`  ... app name is ${appName}.`);

  const appLanguage = retrieveApplicationLanguage();

  if (appLanguage === (AppLanguages.Node as string)) {
    logger.debug('Checking for package-lock.json...');
    const packageLockJsonExists = existsSync(path.join(process.cwd(), 'package-lock.json'));
    logger.debug(`  ... package-lock.json found: ${packageLockJsonExists}`);

    if (!packageLockJsonExists) {
      logger.error("No package-lock.json found. Please run 'npm install' before deploying.");
      return 1;
    }
  } else if (appLanguage === (AppLanguages.Python as string)) {
    logger.debug('Checking for requirements.txt...');
    const requirementsPath = path.join(process.cwd(), 'requirements.txt');
    const requirementsTxtExists = existsSync(requirementsPath);
    logger.debug(`  ... requirements.txt found: ${requirementsTxtExists}`);

    if (!requirementsTxtExists) {
      logger.error('No requirements.txt found. Please create one before deploying.');
      return 1;
    }

    const content = fs.readFileSync(requirementsPath, 'utf8');
    if (!content.includes('dbos')) {
      logger.error(
        "Your requirements.txt does not include 'dbos'. Please make sure you include all your dependencies.",
      );
      return 1;
    }
  } else {
    logger.error(`dbos-config.yaml contains invalid language ${appLanguage}`);
    return 1;
  }

  // First, check if the application exists
  const appRegistered = await isAppRegistered(logger, host, appName, userCredentials);

  // If the app is not registered, register it.
  const interpolatedConfig = readInterpolatedConfig(dbosConfigFilePath, logger);
  const dbosConfig = YAML.parse(interpolatedConfig) as ConfigFile;

  if (appRegistered === undefined) {
    userDBName = await chooseAppDBServer(logger, host, userCredentials, userDBName);
    if (userDBName === '') {
      return 1;
    }

    // Register the app
    if (enableTimeTravel) {
      logger.info('Enabling time travel for this application');
    } else {
      logger.info('Time travel is disabled for this application');
    }
    const ret = await registerApp(userDBName, host, enableTimeTravel, appName);
    if (ret !== 0) {
      return 1;
    }
  } else {
    logger.info(`Application ${appName} exists, updating...`);
    if (userDBName && appRegistered.PostgresInstanceName !== userDBName) {
      logger.warn(
        `Application ${chalk.bold(appName)} is deployed with database instance ${chalk.bold(appRegistered.PostgresInstanceName)}. Ignoring the provided database instance name ${chalk.bold(userDBName)}.`,
      );
    }

    // Make sure the app database is the same.
    if (
      appRegistered.ApplicationDatabaseName &&
      dbosConfig.database?.app_db_name &&
      dbosConfig.database.app_db_name !== appRegistered.ApplicationDatabaseName
    ) {
      logger.error(
        `Application ${chalk.bold(appName)} is deployed with app_db_name ${chalk.bold(appRegistered.ApplicationDatabaseName)}, but ${dbosConfigFilePath} specifies ${chalk.bold(dbosConfig.database.app_db_name)}. Please update the app_db_name field in ${dbosConfigFilePath} to match the database name.`,
      );
      return 1;
    }
  }

  try {
    const body: { application_archive?: string; previous_version?: string; target_database_name?: string } = {};
    if (previousVersion === null) {
      logger.debug('Creating application zip ...');
      body.application_archive = await createZipData(logger);
      logger.debug('  ... application zipped.');
    } else {
      logger.info(`Restoring previous version ${previousVersion}`);
      body.previous_version = previousVersion;
    }

    if (targetDatabaseName !== null) {
      logger.info(`Changing database instance for ${appName} to ${targetDatabaseName} and redeploying`);
      body.target_database_name = targetDatabaseName;
    }

    // Submit the deploy request
    let url = '';
    if (rollback) {
      url = `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}/rollback`;
    } else if (targetDatabaseName !== null) {
      url = `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}/changedbinstance`;
    } else {
      url = `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}`;
    }

    logger.info(`Submitting deploy request for ${appName}`);
    const s = performance.now();
    let uploadStartTime: number | undefined = undefined;
    let uploadEndTime: number | undefined = undefined;
    interface UploadProgressEvent {
      loaded: number;
      total?: number;
    }
    const response = await axios.post(url, body, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
      onUploadProgress: (progressEvent: UploadProgressEvent) => {
        // Handle start of upload
        if (progressEvent.loaded === 0) {
          uploadStartTime = performance.now();
          logger.debug(`Upload started at: ${(uploadStartTime - startTime).toFixed(2)}ms after request init`);
        }

        // Handle completion - ensure total is defined and loaded equals total
        if (progressEvent.total !== undefined && progressEvent.loaded === progressEvent.total) {
          uploadEndTime = performance.now();
          if (uploadStartTime !== undefined) {
            logger.debug(`Upload completed in: ${(uploadEndTime - uploadStartTime).toFixed(2)}ms`);
          }
          logger.debug(`Total time from request init to upload complete: ${(uploadEndTime - s).toFixed(2)}ms`);
        }

        // Log progress percentage only if total is defined
        if (progressEvent.total !== undefined) {
          const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total);
          logger.debug(`Upload progress: ${percentCompleted}%`);
        } else {
          // Alternative logging when total size is unknown
          logger.debug(`Bytes uploaded: ${progressEvent.loaded}`);
        }
      },
    });
    const e: number = performance.now();
    logger.debug(`Total request time (including response): ${(e - s).toFixed(2)}ms`);
    if (uploadEndTime !== undefined) {
      logger.debug(`Time from upload complete to response received: ${(e - uploadEndTime).toFixed(2)}ms`);
    }
    const deployOutput = response.data as DeployOutput;
    logger.info(`Submitted deploy request for ${appName}. Assigned version: ${deployOutput.ApplicationVersion}`);

    // Wait for the application to become available
    let count = 0;
    let applicationAvailable = false;
    while (!applicationAvailable) {
      count += 1;
      if (count % 50 === 0) {
        logger.info(`Waiting for ${appName} with version ${deployOutput.ApplicationVersion} to be available`);
        if (count > 200) {
          logger.info(
            `If ${appName} takes too long to become available, check its logs with 'dbos-cloud applications logs'`,
          );
        }
      }
      if (count > 1800) {
        logger.error('Application taking too long to become available');
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
        if (application.Name === appName && application.Status === 'AVAILABLE') {
          applicationAvailable = true;
        }
      }
      await sleepms(100);
    }
    logger.info(`Successfully deployed ${appName}!`);
    logger.info(`Access your application at https://${userCredentials.organization}-${appName}.${host}/`);
    const endTime = Date.now(); // Record the end time
    const executionTime = endTime - startTime; // Calculate execution time
    logger.debug(`Total deployment time: ${executionTime} ms`); // Pri
    return 0;
  } catch (e) {
    const errorLabel = `Failed to deploy application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
      const resp: CloudAPIErrorResponse = axiosError.response?.data;
      if (resp.DetailedError) {
        logger.error(resp.DetailedError);
      }
      if (resp.message.includes(`application ${appName} not found`)) {
        console.log(
          chalk.red(
            'Did you register this application? Hint: run `dbos-cloud app register -d <database-instance-name>` to register your app and try again',
          ),
        );
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
      return process.env[g1] ?? '';
    }
    if (g1 !== 'PGPASSWORD') {
      logger.warn(`      Variable '${g1}' would be substituted from the process environment, but is not defined.`);
    }
    return '';
  });
}

async function isAppRegistered(
  logger: Logger,
  host: string,
  appName: string,
  userCredentials: DBOSCloudCredentials,
): Promise<Application | undefined> {
  const bearerToken = 'Bearer ' + userCredentials.token;
  let app: Application | undefined = undefined;
  try {
    const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}`, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    });
    app = res.data as Application;
  } catch (e) {
    const errorLabel = `Failed to retrieve info for application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      const resp: CloudAPIErrorResponse = axiosError.response?.data;
      if (resp.message.includes(`application ${appName} not found`)) {
      } else {
        handleAPIErrors(errorLabel, axiosError);
      }
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
  }
  return app;
}
