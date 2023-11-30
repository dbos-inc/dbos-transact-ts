import { DBOSCloudCredentials, dbosEnvPath } from "./login";
import fs from "fs";

export function getCloudCredentials(): DBOSCloudCredentials {
  const userCredentials = JSON.parse(fs.readFileSync(`./${dbosEnvPath}/credentials`).toString("utf-8")) as DBOSCloudCredentials;
  return {
    userName: userCredentials.userName,
    token: userCredentials.token.replace(/\r|\n/g, ""), // Trim the trailing /r /n.
  };
}
