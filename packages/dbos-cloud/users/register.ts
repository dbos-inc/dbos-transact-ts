import { getCloudCredentials, getLogger } from "../cloudutils.js";

export async function registerUser(username: string, secret: string | undefined, host: string): Promise<number> {
  const logger = getLogger();
  await getCloudCredentials(host, logger, username, secret);
  return 0;
}
