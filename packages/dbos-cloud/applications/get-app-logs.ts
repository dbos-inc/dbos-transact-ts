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

export async function getAppLogs(
  host: string,
  last: number,
  pagesize: number,
  appName: string | undefined,
): Promise<number> {
  if (last !== undefined && (isNaN(last) || last <= 0)) {
    throw new Error('The --last parmameter must be an integer greater than 0');
  }
  if (last === undefined) {
    last = 0; //internally, 0 means "get all the logs." This is the default.
  }

  if (pagesize !== undefined && (isNaN(pagesize) || pagesize <= 0)) {
    throw new Error('The --pagesize parmameter must be an integer greater than 0');
  }
  if (pagesize === undefined) {
    pagesize = 1000;
  }

  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;
  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    return 1;
  }

  const url = `https://${host}/v1alpha1/${userCredentials.organization}/logs/applications/${appName}`;
  const headers = {
    'Content-Type': 'application/json',
    Authorization: bearerToken,
  };
  const params = {
    last: last,
    limit: pagesize,
    format: 'json',
  };
  try {
    const res = await axios.get(url, { headers: headers, params: params });
    const logResponse = res.data as LogResponse;
    if (logResponse.end && logResponse.body === '') {
      logger.info(`No logs found for the specified parameters`);
    } else {
      console.log(logResponse.body.trimEnd());
      let more = !logResponse.end;
      let nextTs = logResponse.next_timestamp;
      while (more) {
        const pageParams = {
          limit: pagesize,
          format: 'json',
          since: nextTs,
        };
        const nextPage = await axios.get(url, { headers: headers, params: pageParams });
        const logResponse = nextPage.data as LogResponse;
        console.log(logResponse.body.trimEnd());
        more = !logResponse.end;
        nextTs = logResponse.next_timestamp;
      }
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve logs of application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
