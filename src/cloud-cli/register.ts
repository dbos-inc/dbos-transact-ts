import axios from "axios";
import { createGlobalLogger } from "../telemetry/logs";

export async function registerUser(userName: string, host: string) {
  const logger = createGlobalLogger();
  try {
    // First, register the user.
    const register = await axios.put(
      `http://${host}:8080/user`,
      {
        name: userName,
      },
      {
        headers: {
          "Content-Type": "application/json",
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
  }
  return true;
}
