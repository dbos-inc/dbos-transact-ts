import axios, { AxiosError } from "axios";
import { isCloudAPIErrorResponse, handleAPIErrors, getCloudCredentials, getLogger } from "../cloudutils.js";


export async function orgInvite(host: string, json: boolean) {
    const logger = getLogger();
    const userCredentials = await getCloudCredentials();
    const bearerToken = "Bearer " + userCredentials.token;
    try {
        const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/organizations/secret`, {
            headers: {
              "Content-Type": "application/json",
              Authorization: bearerToken,
            },
          });
      if (json) {
        console.log(JSON.stringify(res.data))
      } else {
        logger.info(`To invite a user to your organization, ${userCredentials.organization}, give them this single-use secret: `);
        logger.info(res.data);
      }
      return 0;
    } catch (e) {
        const errorLabel = `Failed to retrieve invite secret for organization ${userCredentials.organization}`;
      const axiosError = e as AxiosError;
      if (isCloudAPIErrorResponse(axiosError.response?.data)) {
        handleAPIErrors(errorLabel, axiosError);
      } else {
        logger.error(`${errorLabel}: ${(e as Error).message}`);
      }
      return 1;
    }
  }

  export interface OrgUsers {
    readonly OrgName: string;
    readonly UserNames: string[];
  }

  export async function orgListUsers(host: string, json: boolean) {
    const logger = getLogger();
    const userCredentials = await getCloudCredentials();
    const bearerToken = "Bearer " + userCredentials.token;
    try {
        const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/organizations/users`, {
            headers: {
              "Content-Type": "application/json",
              Authorization: bearerToken,
            },
          });
        if (json) {
          console.log(JSON.stringify(res.data))
        } else {
          logger.info(`Users in organization ${userCredentials.organization}: `);
          const orgUsers = res.data as OrgUsers;
          const users = orgUsers.UserNames;
          users.forEach(user => {
            logger.info(user);
          })
        }
      return 0;
    } catch (e) {
        const errorLabel = `Failed to retrieve users for organization ${userCredentials.organization}`;
      const axiosError = e as AxiosError;
      if (isCloudAPIErrorResponse(axiosError.response?.data)) {
        handleAPIErrors(errorLabel, axiosError);
      } else {
        logger.error(`${errorLabel}: ${(e as Error).message}`);
      }
      return 1;
    }
  }
