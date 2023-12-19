import axios from "axios";
import { GlobalLogger } from "../telemetry/logs";
import { getCloudCredentials } from "./utils";

export async function registerUser(username: string, host: string): Promise<number> {
  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  const userName = userCredentials.userName;
  const logger = new GlobalLogger();
  try {
    // First, register the user.
    const register = await axios.put(
      `https://${host}/user`,
      {
        name: userName,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );
    const userUUID = register.data as string;
    logger.info(`Registered user ${userName}, UUID: ${userUUID}`);
  } catch (e) {
    if (axios.isAxiosError(e) && e.response) {
      logger.error(`failed to register user ${userName}: ${e.response.data}`);
    } else {
      logger.error(`failed to register user ${userName}: ${(e as Error).message}`);
    }
    return 1;
  }
  return 0;
}
