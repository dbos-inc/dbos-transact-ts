import axios from "axios";
import fs from "fs";
import YAML from "yaml";
import { createGlobalLogger } from "../../telemetry/logs";
import { getCloudCredentials } from "../utils";
import { ConfigFile, loadConfigFile, dbosConfigFilePath } from "../../dbos-runtime/config";

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
    if (res.status != 200) {
      logger.error("Error getting info for ${dbName} error: ${res.data}.")
      return
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
    const userdbHostname: string = res.data.HostName
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
    const userdbPort: number = res.data.Port

    if (userdbHostname == "" || userdbPort == 0) {
      logger.error("HostName: ${userdbHostname} Port: ${userdbPort} not available.")
      return
    }

    // read the yaml file
    const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
    if (!configFile) {
      logger.error(`failed to parse ${dbosConfigFilePath}`);
      return;
    }

    // update hostname and port
    configFile.database.hostname = userdbHostname
    configFile.database.port = userdbPort

    // save the file
    try {
      fs.writeFileSync(`${dbosConfigFilePath}`, YAML.stringify(configFile));
    } catch (e) {
      logger.error(`failed to write ${dbosConfigFilePath}: ${(e as Error).message}`);
      return;
    }

    logger.info("Successfully configure user database at ${userdbHostname}:${userdbPort}.")

}