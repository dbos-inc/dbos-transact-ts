import { GlobalLogger } from "../telemetry/logs";
import { ConfigFile, constructPoolConfig } from "./config";
import { PoolConfig, Client } from "pg";
import { confirm } from '@inquirer/prompts';

export async function reset(configFile: ConfigFile, logger: GlobalLogger, cnf: boolean) {

  if (cnf) {
    const userConfirmed = await confirm({
      message:
        'This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?',
      default: false, // Default value for confirmation
    });
  
    if (!userConfirmed) {
      console.log('Operation cancelled.');
      process.exit(0); // Exit the process if the user cancels
    }
  }
  
  const userPoolConfig: PoolConfig = constructPoolConfig(configFile)

  const sysDbName = configFile.database.sys_db_name ?? `${userPoolConfig.database}_dbos_sys`;
 
  logger.info(`Resetting ${sysDbName} if it exists`);

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


  await pgClient.query(`DROP DATABASE IF EXISTS ${sysDbName};`);        
  
  await pgClient.end();

  return 0;
}
