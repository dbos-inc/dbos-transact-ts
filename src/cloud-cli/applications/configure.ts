import axios from "axios";
import fs from "fs";
import YAML from "yaml";
import { GlobalLogger } from "../../telemetry/logs";
import { ConfigFile, loadConfigFile, dbosConfigFilePath } from "../../dbos-runtime/config";
import { UserDBInstance, getUserDBInfo } from "../userdb";

export async function configureApp(host: string, port: string, dbName: string): Promise<number> {
    const logger = new GlobalLogger();

    let userDBInfo: UserDBInstance | undefined;
    try {
      userDBInfo = await getUserDBInfo(host, port, dbName);
    } catch(e) {
      logger.error(`error getting DB Info: ${(e as Error).message}`)
      return 1
    }

    const userdbHostname: string = userDBInfo.HostName
    const userdbPort: number = userDBInfo.Port

    if (userdbHostname == "" || userdbPort == 0) {
      logger.error(`HostName: ${userdbHostname} Port: ${userdbPort} not available.`)
      return 1
    }

    // Parse the config file
    const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
    if (!configFile) {
      logger.error(`failed to parse ${dbosConfigFilePath}`);
      return 1;
    }

    const certificateURL = "https://truststore.pki.rds.amazonaws.com/us-east-1/us-east-1-bundle.pem"
    const certificateFile = "us-east-1-bundle.pem"
    try {
      const certificate = await axios.get(certificateURL, {
        responseType: 'arraybuffer'
      });
      fs.writeFileSync(certificateFile, certificate.data as string)
    } catch(e) {
      logger.error("Error downloading RDS certificate bundle. Try downloading it manually from AWS.");
      logger.error((e as Error).message);
      return 1
    }

    // Update database hostname and port
    configFile.database.hostname = userdbHostname
    configFile.database.port = userdbPort
    configFile.database.ssl_ca = certificateFile

    // Save the updated config file
    try {
      fs.writeFileSync(`${dbosConfigFilePath}`, YAML.stringify(configFile));
    } catch (e) {
      logger.error(`failed to write ${dbosConfigFilePath}: ${(e as Error).message}`);
      return 1;
    }

    logger.info(`Successfully configured user database at ${userdbHostname}:${userdbPort}.`)
    return 0
}