import { execSync } from "child_process";
import { TAlgorithm, encode } from "jwt-simple";
import { createGlobalLogger } from "../telemetry/logs";

const operonEnvPath = ".operon";
const secretKey = "SOME SECRET";

interface Session {
  id: number;
  dateCreated: number;
  username: string;
  issued: number;
  expires: number;
}


export function login (userName: string) {
  const logger = createGlobalLogger();
  // TODO: in the future, we should integrate with Okta for login.
  // Generate a valid JWT token based on the userName and store it in the `./.operon/credentials` file.
  // Then the deploy command can retrieve the token from this file.
  logger.info(`Logging in as user: ${userName}`);

  const algorithm: TAlgorithm = "HS256";
  const issued = Date.now();
  const expires = issued + 1000; // Expires after 1 sec.
  const session: Session = {
    id: 1,
    dateCreated: Date.now(),
    username: userName,
    issued: issued,
    expires: expires
  };

  const token = encode(session, secretKey, algorithm);

  execSync(`mkdir -p ${operonEnvPath}`);
  execSync(`echo ${token} > ${operonEnvPath}/credentials`);

  logger.info(`Successfully logged in as user: ${userName}`);
  logger.info(`You can view your credentials in: ./${operonEnvPath}/credentials`);
}