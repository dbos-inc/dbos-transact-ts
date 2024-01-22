import { DBOSCloudCredentials, dbosEnvPath } from "./login";
import fs from "fs";
import { spawn, StdioOptions } from 'child_process';
import { GlobalLogger } from "../telemetry/logs";

export function getCloudCredentials(): DBOSCloudCredentials {
  const logger = new GlobalLogger();
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
