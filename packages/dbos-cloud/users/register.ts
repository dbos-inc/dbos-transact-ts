import { getCloudCredentials, getLogger } from "../cloudutils.js";

export async function registerUser(username: string, secret: string | undefined, host: string): Promise<number> {
  const logger = getLogger();
  const credentials = await getCloudCredentials(host, logger, username, secret);
  if (credentials.userName !== username) {
    logger.warn(`You are trying to register ${username}, but are currently authenticated as ${credentials.userName}. To register a new user, please run "npx dbos-cloud logout".`)
  }
  return 0;
}
