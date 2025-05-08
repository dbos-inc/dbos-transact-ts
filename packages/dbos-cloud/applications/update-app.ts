import axios, { AxiosError } from 'axios';
import {
  handleAPIErrors,
  getCloudCredentials,
  getLogger,
  isCloudAPIErrorResponse,
  retrieveApplicationName,
  DBOSCloudCredentials,
} from '../cloudutils.js';

export async function updateApp(
  host: string,
  appName?: string,
  executorsMemoryMib?: number,
  minVms?: number,
  userCredentials?: DBOSCloudCredentials,
): Promise<number> {
  const logger = getLogger();
  if (!userCredentials) {
    userCredentials = await getCloudCredentials(host, logger);
  }
  const bearerToken = 'Bearer ' + userCredentials.token;

  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    return 1;
  }

  try {
    logger.info(`Updating application: ${appName}`);
    await axios.patch(
      `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}`,
      {
        executors_memory_mib: executorsMemoryMib,
        min_vms: minVms,
      },
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );
    logger.info(`Successfully updated ${appName}!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to update application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
