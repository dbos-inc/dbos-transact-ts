import axios, { AxiosError } from "axios";
import { isCloudAPIErrorResponse, handleAPIErrors, getCloudCredentials, getLogger } from "../cloudutils.js";


export async function orgInvite(host: string) {
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
      
      
      logger.info(`Secret for inviting user to organization ${userCredentials.organization}: `);
      logger.info(res.data);
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

  export async function orgListUsers(host: string) {
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
      
      logger.info(`Users in organization ${userCredentials.organization}: `);    
      logger.info(res.data);
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