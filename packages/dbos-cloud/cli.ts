#!/usr/bin/env node

import {
  registerApp,
  updateApp,
  listApps,
  deleteApp,
  deployAppCode,
  getAppLogs,
} from "./applications/index.js";
import { Command } from 'commander';
import { login } from "./login.js";
import { registerUser } from "./register.js";
import { createUserDb, getUserDb, deleteUserDb, listUserDB, resetDBCredentials, linkUserDB, unlinkUserDB } from "./userdb.js";
import { launchDashboard, getDashboardURL } from "./dashboards.js";
import { DBOSCloudHost, credentialsExist, deleteCredentials } from "./cloudutils.js";
import { getAppInfo } from "./applications/get-app-info.js";
import promptSync from 'prompt-sync';
import chalk from 'chalk';
import fs from "fs";
import { fileURLToPath } from 'url';
import path from "path";
import updateNotifier, { Package } from "update-notifier";

// Read local package.json
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const packageJson = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "package.json")).toString()) as Package;

// Notify the user if the package requires an update.
try {
  const notifier = updateNotifier({
    pkg: packageJson,
    updateCheckInterval: 0
  })
  if (notifier.update && !notifier.update.current.includes("preview") && !notifier.update.current.includes("placeholder") && (notifier.update.current !== notifier.update.latest)) {
    console.log(`
  ${chalk.yellow("-----------------------------------------------------------------------------------------")}

  DBOS Cloud CLI Update available ${chalk.gray(notifier.update.current)} →  ${chalk.green(notifier.update.latest)}

  To upgrade the DBOS Cloud CLI to the latest version, run the following command:
  ${chalk.cyan("`npm i --save-dev @dbos-inc/dbos-cloud@latest`")}

  ${chalk.yellow("-----------------------------------------------------------------------------------------")}`
    );
  }
} catch (error) {
  // Ignore errors in the notifier
}

const program = new Command();

// eslint-disable-next-line @typescript-eslint/no-var-requires
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
  .action(async (options: { username: string }) => {
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
  .description('Deploy this application to the cloud and run associated database migration commands')
  .option('--verbose', 'Verbose log of deployment step')
  .action(async (options: {verbose?: boolean}) => {
    const exitCode = await deployAppCode(DBOSCloudHost, false, options.verbose ?? false);
    process.exit(exitCode);
  });

applicationCommands
  .command('rollback')
  .description('Deploy this application to the cloud and run associated database rollback commands')
  .action(async () => {
    const exitCode = await deployAppCode(DBOSCloudHost, true, false);
    process.exit(exitCode);
  });

applicationCommands
  .command('delete')
  .description('Delete this application')
  .argument('[string]', 'application name')
  .option('--dropdb', 'Drop application database')
  .action(async (appName: string | undefined, options: { dropdb: boolean }) => {
    const exitCode = await deleteApp(DBOSCloudHost, options.dropdb, appName);
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
  .action(async (options: { last: number }) => {
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

const prompt = promptSync({ sigint: true });
databaseCommands
  .command('provision')
  .description("Provision a Postgres database instance")
  .argument('<name>', 'database instance name')
  .requiredOption('-U, --username <string>', 'Specify your database username')
  .option('-W, --password <string>', 'Specify your database user password')
  .action((async (dbname: string, options: { username: string, password: string | undefined }) => {
    if (!options.password) {
      options.password = prompt('Database Password: ', { echo: '*' });
    }
    const exitCode = await createUserDb(DBOSCloudHost, dbname, options.username, options.password, true)
    process.exit(exitCode);
  }))

databaseCommands
  .command('status')
  .description("Retrieve the status of a Postgres database instance")
  .argument('<name>', 'database instance name')
  .option('--json', 'Emit JSON output')
  .action((async (dbname: string, options: { json: boolean }) => {
    const exitCode = await getUserDb(DBOSCloudHost, dbname, options.json)
    process.exit(exitCode);
  }))

databaseCommands
  .command('list')
  .description("List all your Postgres database instances")
  .option('--json', 'Emit JSON output')
  .action((async (options: { json: boolean }) => {
    const exitCode = await listUserDB(DBOSCloudHost, options.json)
    process.exit(exitCode);
  }))

databaseCommands
  .command('reset-password')
  .description("Reset password for a Postgres database instance")
  .argument('<name>', 'database instance name')
  .option('-W, --password <string>', 'Specify the database user password')
  .action((async (dbName: string, options: { password: string }) => {
    if (!options.password) {
      options.password = prompt('Database Password: ', { echo: '*' });
    }
    const exitCode = await resetDBCredentials(DBOSCloudHost, dbName, options.password)
    process.exit(exitCode);
  }))

databaseCommands
  .command('destroy')
  .description("Destroy a Postgres database instance")
  .argument('<name>', 'database instance name')
  .action((async (dbname: string) => {
    const exitCode = await deleteUserDb(DBOSCloudHost, dbname)
    process.exit(exitCode);
  }))

databaseCommands
  .command('link')
  .description("Link your own Postgres database instance to DBOS Cloud")
  .argument('<name>', 'database instance name')
  .option('-H, --hostname <string>', 'Specify your database hostname')
  .option('-p, --port <number>', 'Specify your database port')
  .option('-W, --password <string>', 'Specify password for the dbosadmin user')
  .action((async (dbname: string, options: { hostname: string, port: string, password: string | undefined }) => {
    if (!options.password) {
      options.password = prompt('Database Password: ', { echo: '*' });
    }
    const exitCode = await linkUserDB(DBOSCloudHost, dbname, options.hostname, Number(options.port), options.password)
    process.exit(exitCode);
  }))

databaseCommands
  .command('unlink')
  .description("Unlink a Postgres database instance")
  .argument('<name>', 'database instance name')
  .action((async (dbname: string) => {
    const exitCode = await unlinkUserDB(DBOSCloudHost, dbname)
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
