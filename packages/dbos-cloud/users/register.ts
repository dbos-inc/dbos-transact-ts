import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, credentialsExist, DBOSCloudCredentials, writeCredentials, deleteCredentials } from "../cloudutils.js";
import readline from 'readline';
import validator from 'validator';
import { authenticate } from "./authentication.js";

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

function isValidUsername(username: string): boolean {
  if (username.length < 3 || username.length > 30) {
    return false;
  }
  return validator.matches(username, "^[a-z0-9_]+$");
}

export async function registerUser(username: string, host: string): Promise<number> {
  const logger = getLogger();
  let givenName = "";
  let familyName = "";
  let company = "";
  if (!isValidUsername(username)) {
    logger.error("Invalid username. Usernames must be between 3 and 30 characters long and contain only lowercase letters, underscores, and numbers.")
    return 1
  }
  if (!credentialsExist()) {
    logger.info("Welcome to DBOS Cloud!")
    logger.info("Before creating an account, please tell us a bit about yourself!")
    const prompt = (query: string) => new Promise<string>((resolve) => rl.question(query, resolve));
    givenName = await prompt("Enter First/Given Name: ");
    familyName = await prompt("Enter Last/Family Name: ");
    company = await prompt("Enter Company: ");
    const authResponse = await authenticate(logger);
    if (authResponse === null) {
      return 1
    }
    const credentials: DBOSCloudCredentials = {
      token: authResponse.token,
      userName: username,
    };
    writeCredentials(credentials)
  } else {
    const userCredentials = await getCloudCredentials();
    if (userCredentials.userName !== username) {
      logger.error(`You are trying to register ${username}, but are currently authenticated as ${userCredentials.userName}. Please run "npx dbos-cloud logout".`)
      return 1;
    } else {
      logger.info(`You are currently authenticated as ${userCredentials.userName}.  Registering ${userCredentials.userName} with DBOS Cloud...`)
    }
  }

  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  const loginName = userCredentials.userName;
  try {
    // Register the user in DBOS Cloud
    await axios.put(
      `https://${host}/v1alpha1/user`,
      {
        name: loginName,
        given_name: givenName,
        family_name: familyName,
        company: company,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );
    logger.info(`${username} successfully registered!`);
  } catch (e) {
    const errorLabel = `Failed to register user ${loginName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    if (credentialsExist()) {
      deleteCredentials();
    }
    return 1;
  }
  return 0;
}
