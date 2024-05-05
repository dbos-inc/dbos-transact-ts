import { transports, createLogger, format, Logger } from "winston";
import fs from "fs";
import { AxiosError } from "axios";
import jwt from 'jsonwebtoken';
import path from "node:path";
import { authenticateWithRefreshToken } from "./users/authentication.js";

export interface DBOSCloudCredentials {
  token: string;
  refreshToken?: string;
  userName: string;
}

export interface UserProfile {
  Name: string;
  Email: string;
  Organization: string;
  SubscriptionPlan: string;
}

export const dbosConfigFilePath = "dbos-config.yaml";
export const DBOSCloudHost = process.env.DBOS_DOMAIN || "cloud.dbos.dev";
export const dbosEnvPath = ".dbos";

export function retrieveApplicationName(logger: Logger, silent: boolean = false): string | undefined {
  const packageJson = JSON.parse(fs.readFileSync(path.join(process.cwd(), "package.json")).toString()) as { name: string };
  const appName = packageJson.name;
  if (appName === undefined) {
    logger.error("Error: cannot find a valid package.json file. Please run this command in an application root directory.")
    return undefined;
  }
  if (!silent) {
    logger.info(`Loaded application name from package.json: ${appName}`)
  }
  return appName
}

export type CLILogger = ReturnType<typeof createLogger>;
let curLogger: Logger | undefined = undefined;
export function getLogger(verbose?:boolean): CLILogger {
  if (curLogger) return curLogger;
  const winstonTransports = [];
  winstonTransports.push(
    new transports.Console({
      format: consoleFormat,
      level:  verbose ? "debug" : "info",
    })
  );
  return curLogger = createLogger({ transports: winstonTransports });
}

const consoleFormat = format.combine(
  format.errors({ stack: true }),
  format.timestamp(),
  format.colorize(),
  format.printf((info) => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const { timestamp, level, message, stack } = info;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const ts = timestamp.slice(0, 19).replace("T", " ");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const formattedStack = stack?.split("\n").slice(1).join("\n");

    const messageString: string = typeof message === "string" ? message : JSON.stringify(message);

    return `${ts} [${level}]: ${messageString} ${stack ? "\n" + formattedStack : ""}`;
  })
);

function isTokenExpired(token: string): boolean {
  try {
    const { exp } = jwt.decode(token) as jwt.JwtPayload;
    if (!exp) return false;
    return Date.now() >= exp * 1000;
  } catch (error) {
    return true;
  }
}

export async function getCloudCredentials(): Promise<DBOSCloudCredentials> {
  const logger = getLogger();
  if (!credentialsExist()) {
    logger.error("Error: not logged in")
    process.exit(1)
  }
  const userCredentials = JSON.parse(fs.readFileSync(`./${dbosEnvPath}/credentials`).toString("utf-8")) as DBOSCloudCredentials;
  const credentials =  {
    userName: userCredentials.userName,
    refreshToken: userCredentials.refreshToken,
    token: userCredentials.token.replace(/\r|\n/g, ""), // Trim the trailing /r /n.
  };
  if (isTokenExpired(credentials.token)) {
    if (credentials.refreshToken) {
      logger.info("Refreshing access token with refresh token")
      const authResponse = await authenticateWithRefreshToken(logger, credentials.refreshToken);
      if (authResponse === null) {
        logger.error("Error: Refreshing access token with refresh token failed.  Logging out...");
        deleteCredentials();
        process.exit(1);
      }
      credentials.token = authResponse.token;
      writeCredentials(credentials);
      return await getCloudCredentials();
    } else {
      logger.error("Error: Login expired. Please log in again with 'npx dbos-cloud login'");
      process.exit(1);
    }
  }
  return credentials
}

export function credentialsExist(): boolean {
  return fs.existsSync(`./${dbosEnvPath}/credentials`);
}

export function deleteCredentials() {
  fs.unlinkSync(`./${dbosEnvPath}/credentials`);
}

export function writeCredentials(credentials: DBOSCloudCredentials) {
  fs.mkdirSync(dbosEnvPath, { recursive: true });
  fs.writeFileSync(path.join(dbosEnvPath, 'credentials'), JSON.stringify(credentials), "utf-8");
}

export function checkReadFile(path: string, encoding: BufferEncoding = "utf8"): string | Buffer {
  // First, check the file
  fs.stat(path, (error: NodeJS.ErrnoException | null, stats: fs.Stats) => {
    if (error) {
      throw new Error(`checking on ${path}. ${error.code}: ${error.errno}`);
    } else if (!stats.isFile()) {
      throw new Error(`config file ${path} is not a file`);
    }
  });

  // Then, read its content
  const fileContent: string = fs.readFileSync(path, { encoding } );
  return fileContent;
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export type ValuesOf<T> = T[keyof T];


export function createDirectory(path: string): string | undefined {
  return fs.mkdirSync(path, { recursive: true });
}

export interface CloudAPIErrorResponse {
  message: string,
  statusCode: number,
  requestID: string,
}

export function isCloudAPIErrorResponse(obj: unknown): obj is CloudAPIErrorResponse {
  return typeof obj === 'object' && obj !== null &&
    'message' in obj && typeof obj['message'] === 'string' &&
    'statusCode' in obj && typeof obj['statusCode'] === 'number' &&
    'requestID' in obj && typeof obj['requestID'] === 'string';
}

export function handleAPIErrors(label: string, e: AxiosError) {
  const logger = getLogger();
  const resp: CloudAPIErrorResponse = e.response?.data as CloudAPIErrorResponse;
  logger.error(`[${resp.requestID}] ${label}: ${resp.message}.`);
}
