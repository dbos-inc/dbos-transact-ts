import axios, { AxiosError } from "axios";
import { getLogger, isCloudAPIErrorResponse, handleAPIErrors } from "../cloudutils.js";
import open from "open";
import { getCloudCredentials } from "../cloudutils.js";

export async function launchDashboard(host: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = "Bearer " + userCredentials.token;
  try {
    logger.warn(`'dashboard launch' is a deprecated command; use 'dashboard url' instead.`);
    const res = await axios.put(
      `https://${host}/v1alpha1/${userCredentials.organization}/dashboard`,
      {},
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: bearerToken,
        },
      }
    );
    logger.info(`Dashboard ready at ${res.data}`);
    if (typeof res.data === "string") {
      try {
        await open(res.data);
      } catch (error) {
        /* Ignore errors from open */
      }
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to initialize dashboard`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
  }
  return 1;
}

export async function getDashboardURL(host: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = "Bearer " + userCredentials.token;
  try {
    const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/dashboard`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    logger.info(`Dashboard URL is ${res.data}`);
    if (typeof res.data === "string") {
      try {
        await open(res.data);
      } catch (error) {
        /* Ignore errors from open */
      }
    }

    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve URL`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
  }
  return 1;
}

export async function deleteDashboard(host: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = "Bearer " + userCredentials.token;
  try {
    await axios.delete(`https://${host}/v1alpha1/${userCredentials.organization}/dashboard`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });
    logger.info(`Dashboard deleted`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to delete dashboard`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
  }
  return 1;
}
