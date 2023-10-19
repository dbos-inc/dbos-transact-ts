import { execSync } from "child_process";

const operonEnvPath = ".operon";

export async function login (userName: string) {
  // TODO: in the future, we should integrate with Okta for login.
  // Generate a valid JWT token based on the userName and store it in the `./.operon/credentials` file.
  // Then the deploy command can retrieve the token from this file.
  console.log("Logging in as user: ", userName);

  execSync(`mkdir -p ${operonEnvPath}`);
  execSync(`echo ${userName} > ${operonEnvPath}/credentials`);

  console.log("Successfully logged in as user:", userName);
}