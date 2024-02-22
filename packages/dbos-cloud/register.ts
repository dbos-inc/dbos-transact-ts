import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, credentialsExist } from "./cloudutils";
import readline from 'readline';
import { login } from "./login";

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

export async function registerUser(username: string, host: string): Promise<number> {
  const logger = getLogger();
  let givenName = "";
  let familyName = "";
  let company = "";
  if (!credentialsExist()) {
    logger.info("Welcome to DBOS Cloud!")
    logger.info("Before creating an account, please tell us a bit about yourself!")
    const prompt = (query: string) => new Promise<string>((resolve) => rl.question(query, resolve));
    givenName = await prompt("Enter First/Given Name: ");
    familyName = await prompt("Enter Last/Family Name: ");
    company = await prompt("Enter Company: ");
    const exitCode = await login(username);
    if (exitCode !== 0) {
      return exitCode
    }
  } else {
    const userCredentials = getCloudCredentials();
    if (userCredentials.userName !== username) {
      logger.error(`You are trying to register ${username}, but are currently logged in as ${userCredentials.userName}. Please run "npx dbos-cloud logout".`)
      return 1;
    } else {
      logger.info(`You are currently logged in as ${userCredentials.userName}.  Registering ${userCredentials.userName} with DBOS Cloud...`)
    }
  }

  const userCredentials = getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;
  const loginName = userCredentials.userName;
  try {
    // First, register the user.
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
    return 1;
  }
  return 0;
}
