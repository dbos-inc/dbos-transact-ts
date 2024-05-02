import axios , { AxiosError } from "axios";
import { handleAPIErrors, getCloudCredentials, getLogger, isCloudAPIErrorResponse, UserProfile } from "../cloudutils.js";

export async function profile(host: string, json: boolean): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials();
  const bearerToken = "Bearer " + userCredentials.token;

  try {
    const res = await axios.get(`https://${host}/v1alpha1/user/profile`, {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      }
    });
    const user = res.data as UserProfile
    if (json) {
      console.log(JSON.stringify(user));
    } else {
        console.log(`Name: ${user.Name}`);
        console.log(`Email: ${user.Email}`);
        console.log(`Subscription Plan: ${user.SubscriptionPlan}`);
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve info for user ${userCredentials.userName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
