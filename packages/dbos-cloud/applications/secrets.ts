import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, retrieveApplicationName, DBOSCloudCredentials} from "../cloudutils.js";

export interface CreateSecretRequest {
  ApplicationName: string;
  SecretName: string;
  ClearSecretValue: string;
}

export async function createSecret(host: string, appName: string | undefined, secretName: string, secretValue: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);

  logger.debug("Retrieving app name...");
  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    logger.error("Failed to get app name.");
    return 1;
  }

  if (!secretName) {
    logger.error("Secret name is required.");
    return 1;
  }

    if (!secretValue) {
        logger.error("Secret value is required.");
        return 1;
    }

  const request = {'ApplicationName': appName , 'SecretName':secretName, 'ClearSecretValue': secretValue};
 
  return postCreateSecret(host, userCredentials, request);
}

export async function postCreateSecret(host: string, userCredentials: DBOSCloudCredentials, request: CreateSecretRequest): Promise<number> {
  const logger = getLogger();
  const bearerToken = "Bearer " + userCredentials.token;
  try {
    const res = await axios.post(`https://${host}/v1alpha1/${userCredentials.organization}/applications/secrets`, request, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    
    if (res.status !== 200) {
      logger.error(`Failed to create secret for application ${request.ApplicationName}`);
      return 1;
    }

    logger.info(`Secret ${request.SecretName} successfully created!`);
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

export async function listSecrets(host: string, appName: string | undefined, json: boolean): Promise<number> {
    const logger = getLogger();
    const userCredentials = await getCloudCredentials(host, logger);
    const bearerToken = "Bearer " + userCredentials.token;
  
    logger.debug("Retrieving app name...");
    appName = appName || retrieveApplicationName(logger);
    if (!appName) {
      logger.error("Failed to get app name.");
      return 1;
    }
    logger.debug(`  ... app name is ${appName}.`);
  
    try {
      const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}/secrets`, {
        headers: {
          Authorization: bearerToken,
        },
      });
      
      if (res.status !== 200) {
        logger.error(`Failed to list secret for application ${appName}`);
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