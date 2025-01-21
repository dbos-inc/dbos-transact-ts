import { GlobalLogger } from "../telemetry/logs";
import { ConfigFile, constructPoolConfig } from "./config";
import { PoolConfig, Client } from "pg";
import { createUserDBSchema, userDBIndex, userDBSchema } from "../../schemas/user_db_schema";
import { ExistenceCheck, migrateSystemDatabase } from "../system_database";


export async function migrate(configFile: ConfigFile, logger: GlobalLogger) {
  const sysDBName = configFile.database.sys_db_name;
  logger.info(`Resetting ${sysDBName} if it does not exist`);

}
