import axios, { AxiosError } from 'axios';
import {
  handleAPIErrors,
  getCloudCredentials,
  getLogger,
  isCloudAPIErrorResponse,
  retrieveApplicationName,
} from '../cloudutils.js';
import { DateTime } from 'luxon';

type LogResponse = {
  end: boolean;
  next_timestamp: string;
  body: string;
};

export async function getAppLogs(
  host: string,
  appName: string | undefined,
  options: { last?: number; pagesize?: number; since?: string; upto?: string },
): Promise<number> {
  const since = options.since ? DateTime.fromISO(options.since) : undefined;
  const upto = options.upto ? DateTime.fromISO(options.upto) : undefined;
  const last = options.last;
  const pagesize = options.pagesize ?? 1000;

  if (last !== undefined && (isNaN(last) || last <= 0)) {
    throw new Error('The --last parameter must be an integer greater than 0');
  }
  if (pagesize !== undefined && (isNaN(pagesize) || pagesize <= 0)) {
    throw new Error('The --pagesize parameter must be an integer greater than 0');
  }
  if (since && since.isValid === false) {
    throw new Error('The --since parameter must be an ISO 8601 format timestamp');
  }
  if (upto && upto.isValid === false) {
    throw new Error('The --upto parameter must be an ISO 8601 format timestamp');
  }
  if (since && last) {
    throw new Error('The --last and --since parameters cannot be used together');
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
  const $upto = upto?.toUTC().toFormat('yyyy-MM-dd HH:mm:ss.SSS000');
  const params = {
    format: 'json',
    last: last,
    since: since?.toUTC().toFormat('yyyy-MM-dd HH:mm:ss.SSS000'),
    upto: $upto,
    limit: pagesize,
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
          upto: $upto,
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
