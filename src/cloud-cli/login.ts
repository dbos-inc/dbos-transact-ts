import { createGlobalLogger } from "../telemetry/logs";
import axios from "axios";
import { sleep } from "../utils";

export const operonEnvPath = ".operon";
export const DBOSClientID = 'G38fLmVErczEo9ioCFjVIHea6yd0qMZu'
export const DBOSCloudIdentifier = 'dbos-cloud-api'

export interface OperonCloudCredentials {
  token: string;
  userName: string;
}

interface DeviceCodeResponse {
  device_code: string;
  user_code: string;
  verification_uri: string;
  verification_uri_complete: string;
  expires_in: number;
  interval: number;
}

interface TokenResponse {
  access_token: string;
  token_type: string;
  expires_in: number;
}

export async function login(): Promise<number> {
  const logger = createGlobalLogger();
  logger.info(`Logging in!`);

  const deviceCodeRequest = {
    method: 'POST',
    url: 'https://dbos-inc.us.auth0.com/oauth/device/code',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    data: { client_id: DBOSClientID, scope: 'sub name email', audience: DBOSCloudIdentifier }
  };
  let deviceCodeResponse: DeviceCodeResponse | undefined;
  try {
    const response = await axios.request(deviceCodeRequest);
    deviceCodeResponse = response.data as DeviceCodeResponse;
  } catch (e) {
    logger.error(`failed to log in`, e);
  }
  if (!deviceCodeResponse) {
    return 1;
  }
  console.log(`Login URL: ${deviceCodeResponse.verification_uri_complete}`);

  const tokenRequest = {
    method: 'POST',
    url: 'https://dbos-inc.us.auth0.com/oauth/token',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    data: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:device_code',
      device_code: deviceCodeResponse.device_code,
      client_id: DBOSClientID
    })
  };
  let tokenResponse: TokenResponse | undefined;
  let elapsedTimeSec = 0;
  while (elapsedTimeSec < deviceCodeResponse.expires_in) {
    try {
      await sleep(deviceCodeResponse.interval * 1000)
      elapsedTimeSec += deviceCodeResponse.interval;
      const response = await axios.request(tokenRequest);
      tokenResponse = response.data as TokenResponse;
      break;
    } catch (e) {
      logger.info(`Waiting for login...`);
    }
  }
  if (!tokenResponse) {
    return 1;
  }

  console.log(tokenResponse);
  return 0;
}
