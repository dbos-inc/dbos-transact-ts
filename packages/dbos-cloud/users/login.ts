import axios, { AxiosError } from "axios";
import { CloudAPIErrorResponse, DBOSCloudCredentials, UserProfile, credentialsExist, dbosEnvPath, deleteCredentials, getLogger, handleAPIErrors, isCloudAPIErrorResponse, isTokenExpired, writeCredentials } from "../cloudutils.js";
import { AuthenticationResponse, authenticate, authenticateWithRefreshToken } from "./authentication.js";
import { Logger } from "winston";
import fs, { write } from "fs";
import { input } from "@inquirer/prompts";
import validator from "validator";

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

/**
 * Login and obtain user credentials.
 * First checks if the credentials exist and not expired.
 * If so, returns the credentials.
 * If not, try to login the user.
 * If the user is not registered, prompts to register the user.
 * @param {string} host - The DBOS Cloud host to authenticate against.
 * @returns {DBOSCloudCredentials} - The user's DBOS Cloud credentials.
 */
export async function loginAndGetCredentials(host: string, logger: Logger): Promise<DBOSCloudCredentials> {
  const credentials: DBOSCloudCredentials = {
    token: "",
    refreshToken: "",
    userName: "",
    organization: "",
  };
  
  // Check if credentials exist and are not expired
  let needLogin = true;
  if (credentialsExist()) {
    const userCredentials = JSON.parse(fs.readFileSync(`./${dbosEnvPath}/credentials`).toString("utf-8")) as DBOSCloudCredentials;
    credentials.userName = userCredentials.userName;
    credentials.refreshToken = userCredentials.refreshToken;
    credentials.token = userCredentials.token.replace(/\r|\n/g, ""); // Trim the trailing /r /n.
    credentials.organization = userCredentials.organization;
    logger.debug(`Loaded credentials from ${dbosEnvPath}/credentials`);

    if (isTokenExpired(credentials.token)) {
      if (credentials.refreshToken) {
        logger.info("Refreshing access token with refresh token");
        const authResponse = await authenticateWithRefreshToken(logger, credentials.refreshToken);
        if (authResponse === null) {
          logger.warn("Refreshing access token with refresh token failed. Logging in again...");
          deleteCredentials();
        } else {
          credentials.token = authResponse.token;
        }
        writeCredentials(credentials);
        needLogin = false;
      } else {
        logger.warn("Login expired. Logging in again...");
        deleteCredentials();
      }
    } else {
      needLogin = false; // Credentials exist and are not expired
    }
  }

  if (!needLogin && credentials.userName !== "") {
    logger.debug(`Logged in as ${credentials.userName}`);
    return credentials;
  }

  // Log in the user.
  if (needLogin) {
    const authResponse = await authenticate(logger, false);
    if (authResponse === null) {
      logger.error("Failed to login. Exiting...");
      process.exit(1);
    }
    credentials.token = authResponse.token;
    credentials.refreshToken = authResponse.refreshToken;
  }

  const bearerToken = "Bearer " + credentials.token;

  if (credentials.userName !== "") {
    // Get the user profile
    try {
      const response = await axios.get(`https://${host}/v1alpha1/user/profile`, {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      });
      const profile = response.data as UserProfile;
      credentials.userName = profile.Name;
      credentials.organization = profile.Organization;
      writeCredentials(credentials);
      logger.info(`Successfully logged in as ${credentials.userName}!`);
      return credentials;
    } catch (e) {
      const axiosError = e as AxiosError;
      const errorLabel = `Failed to login`;
      if (isCloudAPIErrorResponse(axiosError.response?.data)) {
        const resp: CloudAPIErrorResponse = axiosError.response?.data;
        if (resp.message.includes("user not found in DBOS Cloud")) {
          logger.info(`User not registered in DBOS Cloud. Registering...`);
        } else {
          handleAPIErrors(errorLabel, axiosError);
          process.exit(1);
        }
      } else {
        logger.error(`${errorLabel}: ${(e as Error).message}`);
        process.exit(1);
      }
    }
  }

  logger.info(`User not registered in DBOS Cloud. Registering...`);

  // Cache the user credentials, but it doesn't have the user name and organization yet.
  // This is designed to avoid extra logins when registering the user next time.
  writeCredentials(credentials);

  // Register the user in DBOS Cloud. Prompt for user name, given name, family name, and company.
  let userName = "";
  // while (userName === "") {
    userName = await input({
      message: "Choose your username:",
      required: true,
      validate: (value: string) => {
        if (value.length < 3 || value.length > 30) {
          return "Username must be 3~30 characters long";
        }
        if (!validator.matches(value, "^[a-z0-9_]+$")) {
          return "Username must contain only lowercase letters, numbers, and underscores.";
        }
        // TODO: Check if the username is already taken. Need a cloud endpoint for this.
        return true;
      }
    })
  // }
  const givenName = await input({
    message: "Enter first/given name:",
    required: true,
  });
  const familyName = await input({
    message: "Enter last/family name:",
    required: true,
  });
  const company = await input({
    message: "Enter company name:",
    required: true,
  });
  
  try {
    await axios.put(
      `https://${host}/v1alpha1/user`,
      {
        name: userName,
        given_name: givenName,
        family_name: familyName,
        company: company, // Currently don't support organizational secrets
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );
    const response = await axios.get(`https://${host}/v1alpha1/user/profile`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    const profile = response.data as UserProfile;
    credentials.userName = profile.Name;
    credentials.organization = profile.Organization;
    writeCredentials(credentials);
    logger.info(` ... Successfully registered and logged in as ${credentials.userName}!`);
  } catch (e) {
    const errorLabel = `Failed to register user ${userName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    process.exit(1);
  }
  return credentials;
}