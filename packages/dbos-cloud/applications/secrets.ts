import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, retrieveApplicationName} from "../cloudutils.js";

export async function createSecret(host: string, appName: string | undefined, secretName: string, secretValue: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = "Bearer " + userCredentials.token;

  logger.debug("Retrieving app name...");
  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    logger.error("Failed to get app name.");
    return 1;
  }
  logger.info(`  ... app name is ${appName}.`);
  logger.info(`  ... secret name is ${secretName}.`);
  logger.info(`  ... secret value is ${secretValue}.`);

  const body = {'ApplicationName': appName , 'SecretName':secretName, 'ClearSecretValue': secretValue};
 
  console.log(body);

  try {
    const res = await axios.post(`https://${host}/v1alpha1/${userCredentials.organization}/applications/secrets`, body, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    
    if (res.status !== 200) {
      logger.error(`Failed to create secret for application ${appName}`);
      return 1;
    }

    logger.info(`Secret successfully created!`);
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
    logger.info(`  ... app name is ${appName}.`);

    logger.info(bearerToken);
  
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
  
      logger.info(res.data);
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