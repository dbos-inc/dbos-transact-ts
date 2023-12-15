import axios from "axios";
import fs from "fs";
import YAML from "yaml";
import { GlobalLogger } from "../../telemetry/logs";
import { ConfigFile, loadConfigFile, dbosConfigFilePath } from "../../dbos-runtime/config";

export async function configureApp(database_host: string, database_port: number, username: string, password: string): Promise<number> {
    const logger = new GlobalLogger();

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
    configFile.database.hostname = database_host
    configFile.database.port = database_port
    configFile.database.ssl_ca = certificateFile
    configFile.database.username = username
    configFile.database.password = password

    // Save the updated config file
    try {
      fs.writeFileSync(`${dbosConfigFilePath}`, YAML.stringify(configFile));
    } catch (e) {
      logger.error(`failed to write ${dbosConfigFilePath}: ${(e as Error).message}`);
      return 1;
    }

    logger.info(`Successfully configured user database at ${database_host}:${database_port}.`)
    return 0
}