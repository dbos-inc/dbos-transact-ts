import axios, { AxiosError } from "axios";
import { DBOSCloudCredentials, UserProfile, getLogger, handleAPIErrors, isCloudAPIErrorResponse, writeCredentials } from "../cloudutils.js";
import { AuthenticationResponse, authenticate, authenticateWithRefreshToken } from "./authentication.js";

export async function login(host: string, getRefreshToken: boolean, useRefreshToken?: string): Promise<number> {
  const logger = getLogger();
  let authResponse: AuthenticationResponse | null;
  if (useRefreshToken) {
    authResponse = await authenticateWithRefreshToken(logger, useRefreshToken);
  } else {
    authResponse = await authenticate(logger, getRefreshToken);
  }
  if (authResponse === null) {
    return 1;
  }
  const bearerToken = "Bearer " + authResponse.token;
  try {
    const response = await axios.get(`https://${host}/v1alpha1/user/profile`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    const profile = response.data as UserProfile;
    const credentials: DBOSCloudCredentials = {
      token: authResponse.token,
      refreshToken: authResponse.refreshToken,
      userName: profile.Name,
      organization: profile.Organization,
    };
    writeCredentials(credentials);
    logger.info(`Successfully logged in as ${credentials.userName}!`);
    if (getRefreshToken) {
      logger.info(`Refresh token saved to .dbos/credentials`);
    }
  } catch (e) {
    const errorLabel = `Failed to login`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
  return 0;
}
