import { transports, createLogger, format, Logger } from 'winston';
import fs from 'fs';
import axios, { AxiosError } from 'axios';
import jwt from 'jsonwebtoken';
import path from 'node:path';
import { loadConfigFile } from './configutils.js';
import { input } from '@inquirer/prompts';
import validator from 'validator';
import { authenticate, authenticateWithRefreshToken } from './users/authentication.js';

export interface DBOSCloudCredentials {
  token: string;
  refreshToken?: string;
  userName: string;
  organization: string;
}

export interface UserProfile {
  Name: string;
  Email: string;
  Organization: string;
  SubscriptionPlan: string;
}

export enum AppLanguages {
  Node = 'node',
  Python = 'python',
  Go = 'go',
}

export const defaultConfigFilePath = 'dbos-config.yaml';
export const DBOSCloudHost = process.env.DBOS_DOMAIN || 'cloud.dbos.dev';
export const dbosEnvPath = '.dbos';

export function retrieveApplicationName(
  logger: Logger,
  silent: boolean = false,
  configFilePath: string = defaultConfigFilePath,
): string | undefined {
  const configFile = loadConfigFile(configFilePath);
  let appName = configFile.name;
  if (appName !== undefined) {
    if (!silent) {
      logger.info(`Loaded application name from dbos-config.yaml: ${appName}`);
    }
    return appName;
  }
  const packageJson = JSON.parse(fs.readFileSync(path.join(process.cwd(), 'package.json')).toString()) as {
    name: string;
  };
  appName = packageJson.name;
  if (appName === undefined) {
    logger.error(
      'Error: cannot find a valid package.json file. Please run this command in an application root directory.',
    );
    return undefined;
  }
  if (!silent) {
    logger.info(`Loaded application name from package.json: ${appName}`);
  }
  return appName;
}

export function retrieveApplicationLanguage(configFilePath: string = defaultConfigFilePath) {
  const configFile = loadConfigFile(configFilePath);
  return configFile.language || AppLanguages.Node;
}

export type CLILogger = ReturnType<typeof createLogger>;
let curLogger: Logger | undefined = undefined;
export function getLogger(verbose?: boolean): CLILogger {
  if (curLogger) return curLogger;
  const winstonTransports = [];
  winstonTransports.push(
    new transports.Console({
      format: consoleFormat,
      level: verbose ? 'debug' : 'info',
    }),
  );
  return (curLogger = createLogger({ transports: winstonTransports }));
}

const consoleFormat = format.combine(
  format.errors({ stack: true }),
  format.timestamp(),
  format.colorize(),
  format.printf((info) => {
    const { timestamp, level, message, stack } = info;
    const ts = typeof timestamp === 'string' ? timestamp.slice(0, 19).replace('T', ' ') : undefined;
    const formattedStack = typeof stack === 'string' ? stack?.split('\n').slice(1).join('\n') : undefined;
    const messageString: string = typeof message === 'string' ? message : JSON.stringify(message);
    return `${ts} [${level}]: ${messageString} ${stack ? '\n' + formattedStack : ''}`;
  }),
);

export function isTokenExpired(token: string): boolean {
  try {
    const { exp } = jwt.decode(token) as jwt.JwtPayload;
    if (!exp) return false;
    return Date.now() >= exp * 1000;
  } catch (error) {
    return true;
  }
}

export function credentialsExist(): boolean {
  return fs.existsSync(`./${dbosEnvPath}/credentials`);
}

export function deleteCredentials() {
  fs.unlinkSync(`./${dbosEnvPath}/credentials`);
}

export function writeCredentials(credentials: DBOSCloudCredentials) {
  fs.mkdirSync(dbosEnvPath, { recursive: true });
  fs.writeFileSync(path.join(dbosEnvPath, 'credentials'), JSON.stringify(credentials), 'utf-8');
}

export function checkReadFile(path: string, encoding: BufferEncoding = 'utf8'): string | Buffer {
  // First, check the file
  fs.stat(path, (error: NodeJS.ErrnoException | null, stats: fs.Stats) => {
    if (error) {
      throw new Error(`checking on ${path}. ${error.code}: ${error.errno}`);
    } else if (!stats.isFile()) {
      throw new Error(`config file ${path} is not a file`);
    }
  });

  // Then, read its content
  const fileContent: string = fs.readFileSync(path, { encoding });
  return fileContent;
}

export const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function createDirectory(path: string): string | undefined {
  return fs.mkdirSync(path, { recursive: true });
}

export interface CloudAPIErrorResponse {
  message: string;
  statusCode: number;
  requestID: string;
  DetailedError?: string;
}

export function isCloudAPIErrorResponse(obj: unknown): obj is CloudAPIErrorResponse {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    'message' in obj &&
    typeof obj['message'] === 'string' &&
    'statusCode' in obj &&
    typeof obj['statusCode'] === 'number' &&
    'requestID' in obj &&
    typeof obj['requestID'] === 'string'
  );
}

export function handleAPIErrors(label: string, e: AxiosError) {
  const logger = getLogger();
  const resp: CloudAPIErrorResponse = e.response?.data as CloudAPIErrorResponse;
  logger.error(`[${resp.requestID}] ${label}: ${resp.message}.`);
}

/**
 * Login and obtain user credentials.
 * First checks if the credentials exist and not expired.
 * If so, returns the credentials.
 * If not, try to login the user.
 * If the user is not registered, prompts to register the user.
 * @param {string} host - The DBOS Cloud host to authenticate against.
 * @returns {DBOSCloudCredentials} - The user's DBOS Cloud credentials.
 */
export async function getCloudCredentials(
  host: string,
  logger: Logger,
  userName?: string,
  secret?: string,
): Promise<DBOSCloudCredentials> {
  // Check if credentials exist and are not expired
  const credentials = await checkCredentials(logger);

  // Log in the user.
  if (credentials.token === '') {
    const authResponse = await authenticate(logger, false);
    if (authResponse === null) {
      logger.error('Failed to login. Exiting...');
      process.exit(1);
    }
    credentials.token = authResponse.token;
    credentials.refreshToken = authResponse.refreshToken;
    // Cache the user credentials, but it doesn't have the user name and organization yet.
    // This is designed to avoid extra logins when registering the user next time.
    writeCredentials(credentials);
  }

  // Check if the user exists in DBOS Cloud
  const userExists = await checkUserProfile(host, credentials, logger);
  if (userExists) {
    writeCredentials(credentials);
    logger.debug(`Successfully logged in as ${credentials.userName}!`);
    return credentials;
  }

  // User doesn't exist, register the user in DBOS Cloud
  await registerUser(host, credentials, logger, userName, secret);
  writeCredentials(credentials);

  return credentials;
}

/**
 * Check if the credentials exist and are not expired.
 * If so, return the credentials.
 * If not, delete the existing credentials and return empty credentials.
 * @param logger - The logger instance.
 * @returns {DBOSCloudCredentials} - The user's DBOS Cloud credentials if exists, or an empty one.
 */
async function checkCredentials(logger: Logger): Promise<DBOSCloudCredentials> {
  const emptyCredentials: DBOSCloudCredentials = { token: '', userName: '', organization: '' };
  if (!credentialsExist()) {
    return emptyCredentials;
  }
  const credentials = JSON.parse(
    fs.readFileSync(`./${dbosEnvPath}/credentials`).toString('utf-8'),
  ) as DBOSCloudCredentials;
  credentials.token = credentials.token.replace(/\r|\n/g, ''); // Trim the trailing /r /n.
  logger.debug(`Loaded credentials from ${dbosEnvPath}/credentials`);
  if (isTokenExpired(credentials.token)) {
    if (credentials.refreshToken) {
      logger.debug('Refreshing access token with refresh token');
      const authResponse = await authenticateWithRefreshToken(logger, credentials.refreshToken);
      if (authResponse === null) {
        logger.warn('Refreshing access token with refresh token failed. Logging in again...');
        deleteCredentials();
        return emptyCredentials;
      } else {
        // Update the token and save the credentials
        credentials.token = authResponse.token.replace(/\r|\n/g, '');
      }
    } else {
      logger.warn('Credentials expired. Logging in again...');
      deleteCredentials();
      return emptyCredentials;
    }
  }
  return credentials;
}

/**
 * Check user profile in DBOS Cloud.
 * @param host - The DBOS Cloud host to authenticate against.
 * @param credentials - The user's DBOS Cloud credentials (to be updated with userName and organization).
 * @param logger  - The logger instance.
 * @returns {boolean} - True if the user profile exists, false otherwise.
 */
async function checkUserProfile(host: string, credentials: DBOSCloudCredentials, logger: Logger): Promise<boolean> {
  const bearerToken = 'Bearer ' + credentials.token;
  try {
    const response = await axios.get(`https://${host}/v1alpha1/user/profile`, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    });
    const profile = response.data as UserProfile;
    credentials.userName = profile.Name;
    credentials.organization = profile.Organization;
    return true;
  } catch (e) {
    const axiosError = e as AxiosError;
    const errorLabel = `Failed to login`;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      const resp: CloudAPIErrorResponse = axiosError.response?.data;
      if (!resp.message.includes('user not found in DBOS Cloud')) {
        handleAPIErrors(errorLabel, axiosError);
        process.exit(1);
      }
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
      process.exit(1);
    }
  }
  return false;
}

function isValidUsername(value: string): boolean | string {
  if (value.length < 3 || value.length > 30) {
    return 'Username must be 3~30 characters long';
  }
  if (!validator.matches(value, '^[a-z0-9_]+$')) {
    return 'Username must contain only lowercase letters, numbers, and underscores.';
  }
  // TODO: Check if the username is already taken. Need a cloud endpoint for this.
  return true;
}

/**
 * Register the user in DBOS Cloud and modify the credentials.
 * Exit the process on errors.
 * @param {string} host - The DBOS Cloud host to authenticate against.
 * @param {DBOSCloudCredentials} credentials - The user's DBOS Cloud credentials (to be updated with userName and organization).
 * @param {Logger} logger - The logger instance.
 * @returns
 */
async function registerUser(
  host: string,
  credentials: DBOSCloudCredentials,
  logger: Logger,
  userName?: string,
  secret?: string,
): Promise<void> {
  logger.info(`User not registered in DBOS Cloud. Registering...`);

  if (!userName) {
    userName = await input({
      message: 'Choose your username:',
      required: true,
      validate: isValidUsername,
    });
  }

  if (isValidUsername(userName) !== true) {
    logger.error(
      `Invalid username: ${userName}. Usernames must be between 3 and 30 characters long and contain only lowercase letters, underscores, and numbers.`,
    );
    process.exit(1);
  }

  let givenName = '',
    familyName = '',
    company = '';
  if (!credentials.userName) {
    // In automated tests, we don't prompt these.
    givenName = await input({
      message: 'Enter first/given name:',
      required: true,
    });
    familyName = await input({
      message: 'Enter last/family name:',
      required: true,
    });
    company = await input({
      message: 'Enter company name:',
      required: true,
    });
  }

  const bearerToken = 'Bearer ' + credentials.token;
  try {
    await axios.put(
      `https://${host}/v1alpha1/user`,
      {
        name: userName,
        given_name: givenName,
        family_name: familyName,
        company: company,
        secret: secret,
      },
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );
    const response = await axios.get(`https://${host}/v1alpha1/user/profile`, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    });
    const profile = response.data as UserProfile;
    credentials.userName = profile.Name;
    credentials.organization = profile.Organization;
    logger.info(` ... Successfully registered and logged in as ${credentials.userName}!`);
  } catch (e) {
    const errorLabel = `Failed to register user ${userName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    process.exit(1);
  }
  return;
}
