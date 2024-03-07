#!/usr/bin/env node

import {
  registerApp,
  updateApp,
  listApps,
  deleteApp,
  deployAppCode,
  getAppLogs,
} from "./applications";
import { Command } from 'commander';
import { login } from "./login";
import { registerUser } from "./register";
import { createUserDb, getUserDb, deleteUserDb, listUserDB, resetDBCredentials } from "./userdb";
import { launchDashboard, getDashboardURL } from "./dashboards";
import { DBOSCloudHost, credentialsExist, deleteCredentials } from "./cloudutils";
import { getAppInfo } from "./applications/get-app-info";

const program = new Command();

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageJson = require('../../../package.json') as { version: string };
program.version(packageJson.version);

/////////////////////
/* AUTHENTICATION  */
/////////////////////

program
  .command('login')
  .description('Log in to DBOS cloud')
  .action(async () => {
    const exitCode = await login(DBOSCloudHost);
    process.exit(exitCode);
  });

program
  .command('register')
  .description('Register a user with DBOS cloud')
  .requiredOption('-u, --username <string>', 'Username')
  .action(async (options: { username: string}) => {
    const exitCode = await registerUser(options.username, DBOSCloudHost);
    process.exit(exitCode);
  });

  program
  .command('logout')
  .description('Log out of DBOS cloud')
  .action(() => {
    if (credentialsExist()) {
      deleteCredentials();
    }
    process.exit(0);
  });

/////////////////////////////
/* APPLICATIONS MANAGEMENT */
/////////////////////////////

const applicationCommands = program
  .command('application')
  .alias('applications')
  .alias('app')
  .alias('apps')
  .description('Manage your DBOS applications')

applicationCommands
  .command('register')
  .description('Register this application')
  .requiredOption('-d, --database <string>', 'Specify a Postgres database instance for this application')
  .action(async (options: { database: string }) => {
    const exitCode = await registerApp(options.database, DBOSCloudHost);
    process.exit(exitCode);
  });

applicationCommands
  .command('update')
  .description('Update this application')
  .action(async () => {
    const exitCode = await updateApp(DBOSCloudHost);
    process.exit(exitCode);
  });

applicationCommands
  .command('deploy')
  .description('Deploy this application to the cloud')
  .action(async () => {
    const exitCode = await deployAppCode(DBOSCloudHost);
    process.exit(exitCode);
  });

applicationCommands
  .command('delete')
  .description('Delete this application')
  .argument('[string]', 'application name')
  .action(async (appName?: string) => {
    const exitCode = await deleteApp(DBOSCloudHost, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('list')
  .description('List all applications')
  .option('--json', 'Emit JSON output')
  .action(async (options: { json: boolean }) => {
    const exitCode = await listApps(DBOSCloudHost, options.json);
    process.exit(exitCode);
  });

applicationCommands
  .command('status')
  .description("Retrieve this application's status")
  .argument('[string]', 'application name')
  .option('--json', 'Emit JSON output')
  .action(async (appName: string | undefined, options: { json: boolean }) => {
    const exitCode = await getAppInfo(DBOSCloudHost, options.json, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('logs')
  .description("Print this application's logs")
  .option('-l, --last <integer>', 'How far back to query, in seconds from current time. By default, we retrieve all data', parseInt)
  .action(async (options: { last: number}) => {
    const exitCode = await getAppLogs(DBOSCloudHost, options.last);
    process.exit(exitCode);
  });

//////////////////////////////////
/* DATABASE INSTANCE MANAGEMENT */
//////////////////////////////////

const databaseCommands = program
  .command('database')
  .alias('databases')
  .alias('db')
  .description('Manage Postgres database instances')

databaseCommands
  .command('provision')
  .description("Provision a Postgres database instance")
  .argument('<string>', 'database instance name')
  .requiredOption('-a, --admin <string>', 'Specify the database user')
  .requiredOption('-W, --password <string>', 'Specify the database user password')
  .action((async (dbname: string, options: { admin: string, password: string }) => {
    const exitCode = await createUserDb(DBOSCloudHost, dbname, options.admin, options.password, true)
    process.exit(exitCode);
  }))

databaseCommands
  .command('status')
  .description("Retrieve the status of a Postgres database instance")
  .argument('<string>', 'database instance name')
  .option('--json', 'Emit JSON output')
  .action((async (dbname: string, options: { json: boolean}) => {
    const exitCode = await getUserDb(DBOSCloudHost, dbname, options.json)
    process.exit(exitCode);
  }))

databaseCommands
  .command('list')
  .description("List all your Postgres database instances")
  .option('--json', 'Emit JSON output')
  .action((async (options: { json: boolean}) => {
    const exitCode = await listUserDB(DBOSCloudHost, options.json)
    process.exit(exitCode);
  }))

databaseCommands
  .command('reset')
  .description("Reset password for a Postgres database instance")
  .argument('<string>', 'database instance name')
  .requiredOption('-W, --password <string>', 'Specify the database user password')
  .action((async (dbName: string, options: { password: string}) => {
    const exitCode = await resetDBCredentials(DBOSCloudHost, dbName, options.password)
    process.exit(exitCode);
  }))

databaseCommands
  .command('destroy')
  .description("Destroy a Postgres database instance")
  .argument('<string>', 'database instance name')
  .action((async (dbname: string) => {
    const exitCode = await deleteUserDb(DBOSCloudHost, dbname)
    process.exit(exitCode);
  }))

/////////////////////
/* USER DASHBOARDS */
/////////////////////

const dashboardCommands = program
  .command('dashboard')
  .description('Manage Monitoring Dashboards')

dashboardCommands
  .command('launch')
  .description('Deploy the Monitoring Dashboard')
  .action(async () => {
    const exitCode = await launchDashboard(DBOSCloudHost);
    process.exit(exitCode);
  });

dashboardCommands
  .command('url')
  .description('Get the URL of your Monitoring Dashboard')
  .action(async () => {
    const exitCode = await getDashboardURL(DBOSCloudHost);
    process.exit(exitCode);
  });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
