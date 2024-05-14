import axios, { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, retrieveApplicationName, CloudAPIErrorResponse } from "../cloudutils.js";
import chalk from "chalk";

export async function registerApp(dbname: string, host: string, appName?: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  appName = appName || retrieveApplicationName(logger);
  if (!appName) {
    return 1;
  }
  logger.info(`Registering application: ${appName}`);

  try {
    const register = await axios.put(
      `https://${host}/v1alpha1/${userCredentials.organization}/applications`,
      {
        name: appName,
        database: dbname,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );
    const uuid = register.data as string;
    logger.info(`${appName} ID: ${uuid}`);
    logger.info(`Successfully registered ${appName}!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to register application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
      const resp: CloudAPIErrorResponse = axiosError.response?.data;
      if (resp.message.includes(`database ${dbname} not found`)) {
        console.log(chalk.red(`Did you provision this database? Hint: run \`npx dbos-cloud db provision ${dbname} -U <database-username>\` to provision the database and try again`));
      }
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
