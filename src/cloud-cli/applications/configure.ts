import axios from "axios";
import { createGlobalLogger } from "../telemetry/logs";
import { getCloudCredentials } from "./utils";

export async function configureApp(host: string, port: string, dbName: string) {
    const logger = createGlobalLogger();
    const userCredentials = getCloudCredentials();
    const bearerToken = "Bearer " + userCredentials.token;
    
    // call cloud and get hostname and port
    const res = await axios.get(`http://${host}:${port}/${userCredentials.userName}/databases/userdb/info/${dbName}`, 
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: bearerToken,
      },
    });



    // if status is not available or no hostname/port print error and exit


    // read the yaml file


    // update hostname and port


    // save the file


}