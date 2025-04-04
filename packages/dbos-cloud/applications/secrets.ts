import axios, { AxiosError } from 'axios';
import dotenv, { DotenvPopulateInput } from 'dotenv';
import dotenvExpand from 'dotenv-expand';
import {
  handleAPIErrors,
  getCloudCredentials,
  getLogger,
  isCloudAPIErrorResponse,
  retrieveApplicationName,
  DBOSCloudCredentials,
} from '../cloudutils.js';
import { readFileSync } from 'fs';

export interface CreateSecretRequest {
  ApplicationName: string;
  SecretName: string;
  ClearSecretValue: string;
}

export async function createSecret(
  host: string,
  appName: string | undefined,
  secretName: string,
  secretValue: string,
): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);

  logger.debug('Retrieving app name...');
  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    logger.error('Failed to get app name.');
    return 1;
  }

  if (!secretName) {
    logger.error('Variable name is required.');
    return 1;
  }

  if (!secretValue) {
    logger.error('Variable value is required.');
    return 1;
  }

  const request = { ApplicationName: appName, SecretName: secretName, ClearSecretValue: secretValue };

  return postCreateSecret(host, userCredentials, request);
}

export async function postCreateSecret(
  host: string,
  userCredentials: DBOSCloudCredentials,
  request: CreateSecretRequest,
): Promise<number> {
  const logger = getLogger();
  const bearerToken = 'Bearer ' + userCredentials.token;
  try {
    const res = await axios.post(
      `https://${host}/v1alpha1/${userCredentials.organization}/applications/secrets`,
      request,
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );

    if (res.status !== 200) {
      logger.error(`Failed to create variable for application ${request.ApplicationName}`);
      return 1;
    }

    logger.info(`Variable ${request.SecretName} successfully updated!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve versions for application ${request.ApplicationName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function importSecrets(host: string, appName: string | undefined, envPath: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    logger.error('Failed to get app name.');
    return 1;
  }

  logger.info(`Importing variables from ${envPath}`);

  const envConfig = readFileSync(envPath, 'utf-8');

  // Parse the content using dotenv and expand it to support interpolation.
  // Supported syntax guide: https://dotenvx.com/docs/env-file
  const parsed = dotenv.parse(envConfig);
  const expandedEnv = { ...process.env } as DotenvPopulateInput;
  const options = {
    processEnv: expandedEnv,
    parsed,
  };
  dotenvExpand.expand(options);

  for (const secret of Object.keys(parsed)) {
    const expandedValue = expandedEnv[secret];
    if (expandedValue === undefined) {
      logger.error(`No value found for variable ${secret}`);
      return 1;
    }
    const request = { ApplicationName: appName, SecretName: secret, ClearSecretValue: expandedValue };
    const exitCode = await postCreateSecret(host, userCredentials, request);
    if (exitCode !== 0) {
      return exitCode;
    }
  }
  return 0;
}

export async function listSecrets(host: string, appName: string | undefined, json: boolean): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  logger.debug('Retrieving app name...');
  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    logger.error('Failed to get app name.');
    return 1;
  }
  logger.debug(`  ... app name is ${appName}.`);

  try {
    const res = await axios.get(
      `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}/secrets`,
      {
        headers: {
          Authorization: bearerToken,
        },
      },
    );

    if (res.status !== 200) {
      logger.error(`Failed to list variables for application ${appName}`);
      return 1;
    }

    if (json) {
      console.log(JSON.stringify(res.data));
    } else {
      const secrets = res.data as string[];
      secrets.forEach((secret) => {
        console.log(secret);
      });
    }

    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve versions for application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
