import TransportStream = require("winston-transport");
import { execSync, spawn, StdioOptions } from 'child_process';
import { transports, createLogger, format, Logger } from "winston";
import fs from "fs";
import { AxiosError } from "axios";
import jwt from 'jsonwebtoken';
import path from "node:path";

export interface DBOSCloudCredentials {
  token: string;
  userName: string;
}

export const dbosConfigFilePath = "dbos-config.yaml";
export const DBOSCloudHost = process.env.DBOS_DOMAIN || "cloud.dbos.dev";
export const dbosEnvPath = ".dbos";

export function retrieveApplicationName(logger: Logger, silent: boolean = false): string | null {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const packageJson = require(path.join(process.cwd(), 'package.json')) as { name: string };
    const appName = packageJson.name;
    if (appName === undefined) {
      logger.error("Error: cannot find a valid package.json file. Please run this command in an application root directory.")
      return null;
    }
    if (!silent) {
      logger.info(`Loaded application name from package.json: ${appName}`)
    }
    return appName
}

// FIXME: we should have a global instance of the logger created in cli.ts
export function getLogger(): Logger {
  const winstonTransports: TransportStream[] = [];
  winstonTransports.push(
    new transports.Console({
      format: consoleFormat,
      level:  "info",
    })
  );
  return createLogger({ transports: winstonTransports });
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

    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
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

export function getCloudCredentials(): DBOSCloudCredentials {
  const logger = getLogger();
  if (!credentialsExist()) {
    logger.error("Error: not logged in")
    process.exit(1)
  }
  const userCredentials = JSON.parse(fs.readFileSync(`./${dbosEnvPath}/credentials`).toString("utf-8")) as DBOSCloudCredentials;
  const credentials =  {
    userName: userCredentials.userName,
    token: userCredentials.token.replace(/\r|\n/g, ""), // Trim the trailing /r /n.
  };
  if (isTokenExpired(credentials.token)) {
    logger.error("Error: Login expired. Please log in again with 'npx dbos-cloud login -u <username>'")
    process.exit(1)
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
  execSync(`mkdir -p ${dbosEnvPath}`);
  fs.writeFileSync(`${dbosEnvPath}/credentials`, JSON.stringify(credentials), "utf-8");
}

// Run a command, streaming its output to stdout
export function runCommand(command: string, args: string[] = []): Promise<number> {
  return new Promise((resolve, reject) => {
      const stdio: StdioOptions = 'inherit';

      const process = spawn(command, args, { stdio });

      process.on('close', (code) => {
          if (code === 0) {
              resolve(code);
          } else {
              reject(new Error(`Command "${command}" exited with code ${code}`));
          }
      });

      process.on('error', (error) => {
          reject(error);
      });
  });
}

export function readFileSync(path: string, encoding: BufferEncoding = "utf8"): string | Buffer {
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

interface CloudAPIErrorResponse {
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

