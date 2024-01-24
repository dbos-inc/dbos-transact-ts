import axios from "axios";
import { sleep } from "../../src/utils";
import jwt, { JwtPayload } from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';
import { execSync } from "child_process";
import fs from "fs";
import { getLogger } from "./cloudutils";

export const dbosEnvPath = ".dbos";
export const DBOSClientID = 'G38fLmVErczEo9ioCFjVIHea6yd0qMZu'
export const DBOSCloudIdentifier = 'dbos-cloud-api'

export interface DBOSCloudCredentials {
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

const client = jwksClient({
  jwksUri: 'https://dbos-inc.us.auth0.com/.well-known/jwks.json'
});

async function getSigningKey(kid: string): Promise<string> {
  const key = await client.getSigningKey(kid);
  return key.getPublicKey();
}

async function verifyToken(token: string): Promise<JwtPayload> {
  const decoded = jwt.decode(token, { complete: true });

  if (!decoded || typeof decoded === 'string' || !decoded.header.kid) {
    throw new Error('Invalid token');
  }

  const signingKey = await getSigningKey(decoded.header.kid);

  return new Promise((resolve, reject) => {
    jwt.verify(token, signingKey, { algorithms: ['RS256'] }, (err, verifiedToken) => {
      if (err) {
        reject(err);
      } else {
        resolve(verifiedToken as JwtPayload);
      }
    });
  });
}

export async function login(username: string): Promise<number> {
  const logger = getLogger();
  logger.info(`Logging in!`);

  const deviceCodeRequest = {
    method: 'POST',
    url: 'https://dbos-inc.us.auth0.com/oauth/device/code',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    data: { client_id: DBOSClientID, scope: 'sub', audience: DBOSCloudIdentifier }
  };
  let deviceCodeResponse: DeviceCodeResponse | undefined;
  try {
    const response = await axios.request(deviceCodeRequest);
    deviceCodeResponse = response.data as DeviceCodeResponse;
  } catch (e) {
    (e as Error).message = `failed to log in: ${(e as Error).message}`;
    logger.error(e);
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

  await verifyToken(tokenResponse.access_token);
  const credentials: DBOSCloudCredentials = {
    token: tokenResponse.access_token,
    userName: username,
  };
  execSync(`mkdir -p ${dbosEnvPath}`);
  fs.writeFileSync(`${dbosEnvPath}/credentials`, JSON.stringify(credentials), "utf-8");
  logger.info(`Successfully logged in as user: ${credentials.userName}`);
  logger.info(`You can view your credentials in: ./${dbosEnvPath}/credentials`);
  return 0;
}
