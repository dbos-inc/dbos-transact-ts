import { OperonCloudCredentials, operonEnvPath } from "./login";
import fs from "fs";

export function getCloudCredentials(): OperonCloudCredentials {
  const userCredentials = JSON.parse(fs.readFileSync(`./${operonEnvPath}/credentials`).toString("utf-8")) as OperonCloudCredentials;
  return {
    userName: userCredentials.userName,
    token: userCredentials.token.replace(/\r|\n/g, ""), // Trim the trailing /r /n.
  };
}

export function credentialsExist(): boolean {
  return fs.existsSync(`./${operonEnvPath}/credentials`);
}
