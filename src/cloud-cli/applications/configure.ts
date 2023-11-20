import axios from "axios";
import fs from "fs";
import YAML from "yaml";
import { createGlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";
import { ConfigFile, loadConfigFile, operonConfigFilePath } from "../../operon-runtime/config";

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
    if (res.status != axios.HttpStatusCode.Ok) {
      logger.error("Error getting info for ${dbName} error: ${res.data}.")
      return
    }

    const userdbHostname = res.data.HostName
    const userdbPort = res.data.Port

    if (userdbHostname == "" || userdbPort == 0) {
      logger.error("HostName: ${userdbHostname} Port: ${userdbPort} not available.")
      return
    }

    // read the yaml file
    const configFile: ConfigFile | undefined = loadConfigFile(operonConfigFilePath);
    if (!configFile) {
      logger.error(`failed to parse ${operonConfigFilePath}`);
      return;
    }

    // update hostname and port
    configFile.database.hostname = userdbHostname
    configFile.database.port = userdbPort

    // save the file
    try {
      fs.writeFileSync(`${operonConfigFilePath}`, YAML.stringify(configFile));
    } catch (e) {
      logger.error(`failed to write ${operonConfigFilePath}: ${(e as Error).message}`);
      return;
    }

    logger.info("Successfully configure user database at ${userdbHostname}:${userdbPort}.")

}