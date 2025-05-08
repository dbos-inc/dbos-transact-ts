import axios, { AxiosError } from 'axios';
import {
  handleAPIErrors,
  getCloudCredentials,
  getLogger,
  isCloudAPIErrorResponse,
  retrieveApplicationName,
  CloudAPIErrorResponse,
  retrieveApplicationLanguage,
  DBOSCloudCredentials,
  defaultConfigFilePath,
} from '../cloudutils.js';
import chalk from 'chalk';

type RegisterAppRequest = {
  name: string;
  database: string;
  language: string;
  provenancedb?: string;
  executors_memory_mib?: number;
};

export async function registerApp(
  dbname: string,
  host: string,
  enableTimetravel: boolean = false,
  appName?: string,
  executorsMemoryMib?: number,
  userCredentials?: DBOSCloudCredentials,
  registerConfigFile: string = defaultConfigFilePath,
): Promise<number> {
  const logger = getLogger();
  if (!userCredentials) {
    userCredentials = await getCloudCredentials(host, logger);
  }
  const bearerToken = 'Bearer ' + userCredentials.token;

  appName = appName || retrieveApplicationName(logger, false, registerConfigFile);
  if (!appName) {
    return 1;
  }
  const appLanguage = retrieveApplicationLanguage(registerConfigFile);

  try {
    logger.info(`Registering application: ${appName}`);
    const body: RegisterAppRequest = {
      name: appName,
      database: dbname,
      language: appLanguage,
      provenancedb: enableTimetravel ? dbname : '',
    };
    if (executorsMemoryMib) {
      body.executors_memory_mib = executorsMemoryMib;
    }

    const register = await axios.put(`https://${host}/v1alpha1/${userCredentials.organization}/applications`, body, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    });
    const uuid = register.data as string;
    logger.info(`${appName} ID: ${uuid}`);
    logger.info(`Successfully registered ${appName}!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to register application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
      const resp: CloudAPIErrorResponse = axiosError.response?.data;
      if (resp.message.includes(`database ${dbname} not found`)) {
        console.log(
          chalk.red(
            `Did you provision this database? Hint: run \`dbos-cloud db provision ${dbname} -U <database-username>\` to provision the database and try again`,
          ),
        );
      }
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
