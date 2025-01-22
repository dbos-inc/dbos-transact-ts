import { GlobalLogger } from "../telemetry/logs";
import { ConfigFile, constructPoolConfig } from "./config";
import { PoolConfig, Client } from "pg";


export async function reset(configFile: ConfigFile, logger: GlobalLogger) {
  
  let userPoolConfig: PoolConfig = constructPoolConfig(configFile)

  let sysDbName = configFile.database.sys_db_name ?? `${userPoolConfig.database}_dbos_sys`;
 
  logger.info(`Resetting ${sysDbName} if it does not exist`);

  const pgClient  = new Client({
    user: userPoolConfig.user,
    host: userPoolConfig.host,
    database: "postgres", // Connect to the default PostgreSQL database
    password: userPoolConfig.password,
    port: userPoolConfig.port,
  });
  
  await pgClient.connect();

  console.log("Terminating connections to system database");


  await pgClient.query(`SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = $1
                AND pid <> pg_backend_pid()`, [sysDbName]);


  // let query = `DROP DATABASE IF EXISTS ${systemPoolConfig.database};`              
  // console.log("Dropping system database");
  // console.log(query);

  await pgClient.query(`DROP DATABASE IF EXISTS ${sysDbName};`);        
  
  await pgClient.end();

  return 0;
}
