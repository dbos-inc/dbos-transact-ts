import axios , { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, retrieveApplicationName } from "../cloudutils.js";

export async function getAppLogs(host: string, last: number, pagesize:number): Promise<number> {
  if (last != undefined && (isNaN(last) || last <= 0)) {
    throw new Error('The --last parmameter must be an integer greater than 0');
  }
  if (last == undefined) {
    last = 0      //internally, 0 means "get all the logs." This is the default.
  }

  if (pagesize != undefined && (isNaN(pagesize) || pagesize <= 0)) {
    throw new Error('The --pagesize parmameter must be an integer greater than 0');
  }
  if (pagesize == undefined) {
    pagesize = 1000
  }

  const logger = getLogger();
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  const appName = retrieveApplicationName(logger);
  const url = `https://${host}/v1alpha1/${userCredentials.userName}/logs/applications/${appName}`;
  const headers = {
    "Content-Type": "application/json",
    Authorization: bearerToken,
  }
  const params = {
    last: last,
    limit: pagesize,
    format: 'json'
  }
  if (!appName) {
    return 1;
  }
  try {
    const res = await axios.get(url, {headers: headers, params: params});
    if (res.data.end && res.data.body == "") {
      logger.info(`No logs found for the specified parameters`);
    } else {
      console.log(res.data.body.trimEnd())
      let more = !res.data.end
      let nextTs = res.data.next_timestamp
      while (more) {
        let pageParams = {
          limit: pagesize,
          format: 'json',
          since: nextTs
        }
        let nextPage = await axios.get(url, {headers: headers, params: pageParams});
        console.log(nextPage.data.body.trimEnd())
        more = !nextPage.data.end
        nextTs = nextPage.data.next_timestamp
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
