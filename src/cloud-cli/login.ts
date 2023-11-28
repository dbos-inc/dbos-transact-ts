import { createGlobalLogger } from "../telemetry/logs";
import axios from "axios";

export const operonEnvPath = ".operon";

export interface OperonCloudCredentials {
  token: string;
  userName: string;
}

interface Session {
  id: number;
  dateCreated: number;
  username: string;
  issued: number;
  expires: number;
}

export async function login(): Promise<number> {
  const logger = createGlobalLogger();
  logger.info(`Logging in!`);

  try {
    const options = {
      method: 'POST',
      url: 'https://dbos-inc.us.auth0.com/oauth/device/code',
      headers: { 'content-type': 'application/x-www-form-urlencoded' },
      data: { client_id: 'G38fLmVErczEo9ioCFjVIHea6yd0qMZu', scope: 'sub name email', audience: 'dbos-cloud-api' }
    };
    const response = await axios.request(options);
    console.log(response.data)
    return 0;
  } catch (e) {
    logger.error(`failed to log in`, e);
    return 1;
  }
}
