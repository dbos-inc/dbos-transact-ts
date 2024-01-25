import { DBOSCloudCredentials, dbosEnvPath } from "./login";
import TransportStream = require("winston-transport");
import fs from "fs";
import { spawn, StdioOptions } from 'child_process';
import { transports, createLogger, format, Logger } from "winston";

export const dbosConfigFilePath = "dbos-config.yaml";

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

export function getCloudCredentials(): DBOSCloudCredentials {
  const logger = getLogger();
  if (!credentialsExist()) {
    logger.error("Error: not logged in")
    process.exit(1)
  }
  const userCredentials = JSON.parse(fs.readFileSync(`./${dbosEnvPath}/credentials`).toString("utf-8")) as DBOSCloudCredentials;
  return {
    userName: userCredentials.userName,
    token: userCredentials.token.replace(/\r|\n/g, ""), // Trim the trailing /r /n.
  };
}

export function credentialsExist(): boolean {
  return fs.existsSync(`./${dbosEnvPath}/credentials`);
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

export interface CloudAPIErrorResponse {
  message: string,
  statusCode: number,
  requestID: string,
}

