import axios, { AxiosError } from 'axios';
import {
  handleAPIErrors,
  getCloudCredentials,
  getLogger,
  isCloudAPIErrorResponse,
  retrieveApplicationName,
} from '../cloudutils.js';

type CmdResponse = {
  stdout: string;
  stderr: string;
  exitCode: number;
};

export async function vmCmd(
  host: string,
  executorId: string,
  command: string,
  appName: string | undefined,
): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;
  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    return 1;
  }

  const url = `https://${host}/vmsadmin/${userCredentials.organization}/applications/${appName}/vms/${executorId}/vmcmd`;
  const headers = {
    'Content-Type': 'application/json',
    Authorization: bearerToken,
  };
  const body = {
    command: command,
  };
  try {
    const res = await axios.post(url, body, { headers: headers });
    const response = res.data as CmdResponse;
    if (response.stdout.length > 0) {
      console.log(response.stdout.trimEnd());
    }
    if (response.stderr.length > 0) {
      console.error(response.stderr.trimEnd());
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to execute command`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
