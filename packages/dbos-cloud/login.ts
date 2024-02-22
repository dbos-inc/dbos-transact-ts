import axios, { AxiosError } from "axios";
import jwt, { JwtPayload } from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';
import { DBOSCloudCredentials, getLogger, handleAPIErrors, isCloudAPIErrorResponse, sleep, writeCredentials } from "./cloudutils";
import { Logger } from "winston";

const DBOSCloudHost = process.env.DBOS_DOMAIN || "cloud.dbos.dev";
const productionEnvironment = DBOSCloudHost === "cloud.dbos.dev";
const Auth0Domain = productionEnvironment ? 'login.dbos.dev' : 'dbos-inc.us.auth0.com';
const DBOSClientID = productionEnvironment ? '6p7Sjxf13cyLMkdwn14MxlH7JdhILled' : 'G38fLmVErczEo9ioCFjVIHea6yd0qMZu';
const DBOSCloudIdentifier = 'dbos-cloud-api';

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
  jwksUri: `https://${Auth0Domain}/.well-known/jwks.json`
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

// Redirect a user to auth0 to authenticate, retrieving a JWT bearer token
export async function authenticate(logger: Logger): Promise<string | null> {
  logger.info(`Please authenticate with DBOS Cloud!`);

  const deviceCodeRequest = {
    method: 'POST',
    url: `https://${Auth0Domain}/oauth/device/code`,
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
    return null;
  }
  console.log(`Login URL: ${deviceCodeResponse.verification_uri_complete}`);

  const tokenRequest = {
    method: 'POST',
    url: `https://${Auth0Domain}/oauth/token`,
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
    return null;
  }

  await verifyToken(tokenResponse.access_token);
  return tokenResponse.access_token
}

export async function login(host: string): Promise<number> {
    const logger = getLogger();
    const bearerToken = await authenticate(logger)
    if (bearerToken === null) {
      return 1;
    }
    try {
      const response = await axios.put(
        `https://${host}/v1alpha1/user`,
        {
        },
        {
          headers: {
            "Content-Type": "application/json",
            Authorization: bearerToken,
          },
        }
      );
      const username = response.data as string;
      const credentials: DBOSCloudCredentials = {
        token: bearerToken,
        userName: username,
      };
      writeCredentials(credentials)
      logger.info(`Successfully logged in as ${credentials.userName}!`);
    } catch (e) {
      const errorLabel = `Failed to login`;
      const axiosError = e as AxiosError;
      if (isCloudAPIErrorResponse(axiosError.response?.data)) {
        handleAPIErrors(errorLabel, axiosError);
      } else {
        logger.error(`${errorLabel}: ${(e as Error).message}`);
      }
      return 1;
    }
    return 0;
}