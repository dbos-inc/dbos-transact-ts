import axios, { AxiosError } from 'axios';
import {
  handleAPIErrors,
  getCloudCredentials,
  getLogger,
  isCloudAPIErrorResponse,
} from '../cloudutils.js';

type ResourceDataPoint = {
  application_id: string;
  application_name: string;
  application_version: string;
  timestamp: string;
  vm_count: number;
  memory_avg_mb: number;
  memory_max_mb: number;
  cpu_utilization_pct: number;
};

type ResourceResponse = {
  since: string;
  upto: string;
  group_by: string;
  data: ResourceDataPoint[];
}

export async function getResourceUsage(
  host: string,
  since: string | undefined,
  upto: string | undefined,
  groupBy: string | undefined
): Promise<number> {
  const logger = getLogger();
  // Compose default "since" and "upto": the most recent full minute
  const finishedMinute = new Date();
  finishedMinute.setSeconds(0, 0);
  finishedMinute.setMinutes(finishedMinute.getMinutes() - 1);
  const finishedMinutePlusOne = new Date(finishedMinute);
  finishedMinutePlusOne.setSeconds(59, 999);
  if (since === undefined) {
    since = finishedMinute.toISOString().replace('T', ' ').replace('Z', '').padEnd(26, '0');
  }
  if (upto === undefined) {
    upto = finishedMinutePlusOne.toISOString().replace('T', ' ').replace('Z', '').padEnd(26, '9');
  }
  if (groupBy === undefined) {
    groupBy = 'minute'
  }
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;
  const url = `https://${host}/v1alpha1/${userCredentials.organization}/resource_utilization`;
  const headers = {
     'Content-Type': 'application/json',
     Authorization: bearerToken,
  };
  const params = {
    since: since,
    upto:  upto ,
    group_by: groupBy
  };
  try {
    const res = await axios.get(url, { headers: headers, params: params });
    const response: ResourceResponse = {
      since: since, 
      upto: upto,
      group_by: groupBy,
      data: res.data as ResourceDataPoint[]
    }
    if (response.data.length == 0) {
      logger.info(`No vm usage found for the specified parameters`);
    } else {
      console.log(JSON.stringify(response, null, 2)); 
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve resource usage data`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1; 
  }
}