import axios from "axios";
import jwt, { JwtPayload } from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';
import { Logger } from "winston";
import open from 'open';

const DBOSCloudHost = process.env.DBOS_DOMAIN || "cloud.dbos.dev";
const productionEnvironment = DBOSCloudHost === "cloud.dbos.dev";
const Auth0Domain = productionEnvironment ? 'login.dbos.dev' : 'dbos-inc.us.auth0.com';
const DBOSClientID = productionEnvironment ? '6p7Sjxf13cyLMkdwn14MxlH7JdhILled' : 'G38fLmVErczEo9ioCFjVIHea6yd0qMZu';
const DBOSCloudIdentifier = 'dbos-cloud-api';
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface DeviceCodeResponse {
  device_code: string;
  user_code: string;
  verification_uri: string;
  verification_uri_complete: string;
  expires_in: number;
  interval: number;
}

interface RefreshTokenAuthResponse {
  access_token: string;
  scope: string;
  expires_in: number;
  token_type: string;
}

interface TokenResponse {
  access_token: string;
  token_type: string;
  expires_in: number;
  refresh_token?: string;
}

export interface AuthenticationResponse {
  token: string
  refreshToken?: string
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
export async function authenticate(logger: Logger, getRefreshToken: boolean = false): Promise<AuthenticationResponse | null> {
  logger.info(`Please authenticate with DBOS Cloud!`);

  const deviceCodeRequest = {
    method: 'POST',
    url: `https://${Auth0Domain}/oauth/device/code`,
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    data: {
      client_id: DBOSClientID,
      scope: getRefreshToken ?  'offline_access': 'sub',
      audience: DBOSCloudIdentifier
    }
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

  const loginURL = deviceCodeResponse.verification_uri_complete
  console.log(`Login URL: ${loginURL}`);
  try {
    await open(loginURL)
  } catch (error) { /* Ignore errors from open */ }

  const tokenRequest = {
    method: 'POST',
    url: `https://${Auth0Domain}/oauth/token`,
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    data: {
      grant_type: 'urn:ietf:params:oauth:grant-type:device_code',
      device_code: deviceCodeResponse.device_code,
      client_id: DBOSClientID
    }
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
  return {
    token: tokenResponse.access_token,
    refreshToken: tokenResponse.refresh_token,
  }
}

export async function authenticateWithRefreshToken(logger: Logger, refreshToken: string): Promise<AuthenticationResponse | null> {
  const authenticationRequest = {
    method: 'POST',
    url: `https://${Auth0Domain}/oauth/token`,
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    data: {
      grant_type: "refresh_token",
      client_id: DBOSClientID,
      refresh_token: refreshToken
    }
  };
  try {
    const response = await axios.request(authenticationRequest);
    const responseData = response.data as RefreshTokenAuthResponse;
    return {
      token: responseData.access_token,
      refreshToken: refreshToken,
    }
  } catch (e) {
    (e as Error).message = `Failed to authenticate with refresh token: ${(e as Error).message}`;
    logger.error(e);
    return null;
  }
}

export async function revokeRefreshToken(logger: Logger, refreshToken: string): Promise<number> {
  const request = {
    method: 'POST',
    url: `https://${Auth0Domain}/oauth/revoke`,
    headers: {'content-type': 'application/json'},
    data: {
      client_id: DBOSClientID,
      token: refreshToken
    }
  };
  try {
    await axios.request(request);
    return 0;
  } catch (e) {
    (e as Error).message = `Failed to revoke refresh token: ${(e as Error).message}`;
    logger.error(e);
    return 1;
  }
}