#!/usr/bin/env node

import {
  registerApp,
  listApps,
  deleteApp,
  deployAppCode,
  getAppLogs,
} from "./applications/index.js";
import { Command } from 'commander';
import { login } from "./users/login.js";
import { registerUser } from "./users/register.js";
import { createUserDb, getUserDb, deleteUserDb, listUserDB, resetDBCredentials, linkUserDB, unlinkUserDB, restoreUserDB } from "./databases/databases.js";
import { launchDashboard, getDashboardURL } from "./dashboards/dashboards.js";
import { DBOSCloudHost, credentialsExist, deleteCredentials, getLogger } from "./cloudutils.js";
import { getAppInfo } from "./applications/get-app-info.js";
import promptSync from 'prompt-sync';
import chalk from 'chalk';
import fs from "fs";
import { fileURLToPath } from 'url';
import path from "path";
import updateNotifier, { Package } from "update-notifier";
import { profile } from "./users/profile.js";
import { revokeRefreshToken } from "./users/authentication.js";
import { listAppVersions } from "./applications/list-app-versions.js";
import { orgInvite, orgListUsers } from "./organizations/organization.js";

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

program.version(packageJson.version);

/////////////////////
/* AUTHENTICATION  */
/////////////////////

program
  .command('login')
  .description('Log in to DBOS cloud')
  .option('--get-refresh-token', 'Get a refresh token you can use to programatically log in')
  .option('--with-refresh-token <token>', 'Use a refresh token to programatically log in')
  .action(async (options: { getRefreshToken?: boolean, withRefreshToken?: string }) => {
    const exitCode = await login(DBOSCloudHost, options.getRefreshToken || false, options.withRefreshToken);
    process.exit(exitCode);
  });

program
  .command('profile')
  .description('Get user information')
  .option('--json', 'Emit JSON output')
  .action(async (options: { json: boolean }) => {
    const exitCode = await profile(DBOSCloudHost, options.json);
    process.exit(exitCode);
  });


program
  .command('register')
  .description('Register a user with DBOS cloud')
  .requiredOption('-u, --username <string>', 'Username')
  .option('-s, --secret <string>','Organization secret')
  .action(async (options: { username: string, secret: string }) => {
    const exitCode = await registerUser(options.username, options.secret, DBOSCloudHost);
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

  program
  .command('revoke')
  .description('Revoke a refresh token')
  .argument('<token>', 'Token to revoke')
  .action(async (token: string) => {
    const exitCode = await revokeRefreshToken(getLogger(), token);
    process.exit(exitCode);
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
  .argument('[string]', 'application name (Default: name from package.json)')
  .requiredOption('-d, --database <string>', 'Specify a Postgres database instance for this application')
  .action(async (appName: string | undefined, options: { database: string }) => {
    const exitCode = await registerApp(options.database, DBOSCloudHost, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('deploy')
  .description('Deploy this application to the cloud and run associated database migration commands')
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--verbose', 'Verbose log of deployment step')
  .option('-p, --previous-version <string>', 'Specify a previous version to restore')
  .action(async (appName: string | undefined, options: {verbose?: boolean, previousVersion?: string}) => {
    const exitCode = await deployAppCode(DBOSCloudHost, false, options.previousVersion ?? null, options.verbose ?? false, null, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('rollback')
  .description('Deploy this application to the cloud and run associated database rollback commands')
  .argument('[string]', 'application name (Default: name from package.json)')
  .action(async (appName: string | undefined) => {
    const exitCode = await deployAppCode(DBOSCloudHost, true, null, false, null, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('change-database-instance')
  .description('Change this application\'s database instance and redeploy it')
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--verbose', 'Verbose log of deployment step')
  .option('-p, --previous-version <string>', 'Specify a previous version to restore')
  .requiredOption('-d, --database <string>', 'Specify the new database instance name for this application')
  .action(async (appName: string | undefined, options: {verbose?: boolean, previousVersion?: string, database: string}) => {
    const exitCode = await deployAppCode(DBOSCloudHost, false, options.previousVersion ?? null, options.verbose ?? false, options.database, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('delete')
  .description('Delete this application')
  .argument('[string]', 'application name (Default: name from package.json)')
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
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--json', 'Emit JSON output')
  .action(async (appName: string | undefined, options: { json: boolean }) => {
    const exitCode = await getAppInfo(DBOSCloudHost, options.json, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('versions')
  .description("Retrieve a list of an application's versions")
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--json', 'Emit JSON output')
  .action(async (appName: string | undefined, options: { json: boolean }) => {
    const exitCode = await listAppVersions(DBOSCloudHost, options.json, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('logs')
  .description("Print this application's logs")
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('-l, --last <integer>', 'How far back to query, in seconds from current time. By default, we retrieve all data', parseInt)
  .option('-p, --pagesize <integer>', 'How many lines to fetch at once when paginating. Default is 1000', parseInt)
  .action(async (appName: string | undefined, options: { last: number, pagesize: number}) => {
    const exitCode = await getAppLogs(DBOSCloudHost, options.last, options.pagesize, appName);
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
  .command('restore')
  .description("Restore a Postgres database instance to a specified point in time")
  .argument('<name>', 'database instance name')
  .requiredOption('-t, --restore-time <string>', 'Specify the point in time to which to restore the database. Must be a timestamp in RFC 3339 format. Example: 2009-09-07T23:45:00Z')
  .requiredOption('-n, --target-name <string>', 'Specify the new database instance name')
  .action((async (dbname: string, options: { restoreTime: string, targetName: string}) => {
    const exitCode = await restoreUserDB(DBOSCloudHost, dbname, options.targetName, options.restoreTime, true);
    process.exit(exitCode)
  }))

databaseCommands
  .command('link')
  .description("Link your own Postgres database instance to DBOS Cloud")
  .argument('<name>', 'database instance name')
  .requiredOption('-H, --hostname <string>', 'Specify your database hostname')
  .option('-p, --port <number>', 'Specify your database port', '5432')
  .option('-W, --password <string>', 'Specify password for the dbosadmin user')
  .option('--enable-timetravel', 'Enable time travel on the linked database', false)
  .action((async (dbname: string, options: { hostname: string, port: string, password: string | undefined, enableTimetravel: boolean }) => {
    if (!options.password) {
      options.password = prompt('Password for the dbosadmin user: ', { echo: '*' });
    }
    const exitCode = await linkUserDB(DBOSCloudHost, dbname, options.hostname, Number(options.port), options.password, options.enableTimetravel);
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

////////////////////////////
/* ORGANIZATIONS COMMANDS */
////////////////////////////

const orgCommands = program
  .command('organization')
  .alias('organizations')
  .alias('org')
  .description('Manage dbos organizations')

  orgCommands
  .command('invite')
  .description("Generate an invite secret for a user to join your organization")
  .option('--json', 'Emit JSON output')
  .action((async (options: { json: boolean }) => {
    const exitCode = await orgInvite(DBOSCloudHost, options.json);
    process.exit(exitCode);
  }))

orgCommands
  .command('list')
  .description("List users in your organization")
  .option('--json', 'Emit JSON output')
  .action((async (options: { json: boolean }) => {
    const exitCode = await orgListUsers(DBOSCloudHost, options.json);
    process.exit(exitCode);
  }))

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}


