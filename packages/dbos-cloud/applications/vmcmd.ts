import axios, { AxiosError } from 'axios';
import {
  handleAPIErrors,
  getCloudCredentials,
  getLogger,
  isCloudAPIErrorResponse,
  retrieveApplicationName,
} from '../cloudutils.js';

type LogResponse = {
  end: boolean;
  next_timestamp: string;
  body: string;
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

  type CmdResponse = {
    stdout: string;
    stderr: string;
    exitCode: number;
  };

  const url = `https://${host}/vmsadmin/${userCredentials.organization}/applications/${appName}/vms/${executorId}/vmcmd`;
  const headers = {
    'Content-Type': 'application/json',
    Authorization: bearerToken,
  };
  const params = {
    command: command,
  };
  try {
    const res = await axios.post(url, { headers: headers, params: params });
    const response = res.data as CmdResponse;
    console.log(response.stdout.trimEnd());
    console.error(response.stderr.trimEnd());
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
