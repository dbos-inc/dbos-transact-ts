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
import { createUserDb, getUserDb, deleteUserDb, listUserDB } from "./userdb";
import { DBOSCloudHost } from "./cloudutils";
import { initDashboard } from "./dashboards";
import { getAppInfo } from "./applications/get-app-info";


const program = new Command();

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageJson = require('../../../package.json') as { version: string };
program.
  version(packageJson.version);

/////////////////////
/* AUTHENTICATION  */
/////////////////////

program
  .command('login')
  .description('Log in to DBOS cloud')
  .requiredOption('-u, --username <string>', 'Username')
  .action(async (options: { username: string }) => {
    const exitCode = await login(options.username);
    process.exit(exitCode);
  });

program
  .command('register')
  .description('Register a user and log in to DBOS cloud')
  .requiredOption('-u, --username <string>', 'Username')
  .action(async (options: { username: string}) => {
    const exitCode = await registerUser(options.username, DBOSCloudHost);
    process.exit(exitCode);
  });

/////////////////////////////
/* APPLICATIONS MANAGEMENT */
/////////////////////////////

const applicationCommands = program
  .command('applications')
  .description('Manage your DBOS applications')

applicationCommands
  .command('register')
  .description('Register a new application')
  .requiredOption('-d, --database <string>', 'Specify the app database name')
  .action(async (options: { database: string }) => {
    const exitCode = await registerApp(options.database, DBOSCloudHost);
    process.exit(exitCode);
  });

applicationCommands
  .command('update')
  .description('Update an application')
  .action(async () => {
    const exitCode = await updateApp(DBOSCloudHost);
    process.exit(exitCode);
  });

applicationCommands
  .command('deploy')
  .description('Deploy an application code to the cloud')
  .option('--no-docker', 'Build the code locally without using Docker')
  .action(async (options: { docker: boolean }) => {
    const exitCode = await deployAppCode(DBOSCloudHost, options.docker);
    process.exit(exitCode);
  });

applicationCommands
  .command('delete')
  .description('Delete a previously deployed application')
  .action(async () => {
    const exitCode = await deleteApp(DBOSCloudHost);
    process.exit(exitCode);
  });

applicationCommands
  .command('list')
  .description('List all deployed applications')
  .option('--json', 'Emit JSON output')
  .action(async (options: { json: boolean }) => {
    const exitCode = await listApps(DBOSCloudHost, options.json);
    process.exit(exitCode);
  });

  applicationCommands
  .command('get')
  .description('Get application info')
  .option('--json', 'Emit JSON output')
  .action(async (options: { json: boolean }) => {
    const exitCode = await getAppInfo(DBOSCloudHost, options.json);
    process.exit(exitCode);
  });

applicationCommands
  .command('logs')
  .description('Print the microVM logs of a deployed application')
  .option('-l, --last <integer>', 'How far back to query, in seconds from current time. By default, we retrieve all data', parseInt)
  .action(async (options: { last: number}) => {
    const exitCode = await getAppLogs(DBOSCloudHost, options.last);
    process.exit(exitCode);
  });

//////////////////////////////
/* USER DATABASE MANAGEMENT */
//////////////////////////////

const userdbCommands = program
  .command('userdb')
  .description('Manage your databases')

userdbCommands
  .command('create')
  .argument('<string>', 'database name')
  .requiredOption('-a, --admin <string>', 'Specify the admin user')
  .requiredOption('-W, --password <string>', 'Specify the admin password')
  .option('-s, --sync', 'make synchronous call', true)
  .action((async (dbname: string, options: { admin: string, password: string, sync: boolean }) => {
    const exitCode = await createUserDb(DBOSCloudHost, dbname, options.admin, options.password, options.sync)
    process.exit(exitCode);
  }))

userdbCommands
  .command('status')
  .argument('<string>', 'database name')
  .option('--json', 'Emit JSON output')
  .action((async (dbname: string, options: { json: boolean}) => {
    const exitCode = await getUserDb(DBOSCloudHost, dbname, options.json)
    process.exit(exitCode);
  }))

  userdbCommands
  .command('list')
  .option('--json', 'Emit JSON output')
  .action((async (options: { json: boolean}) => {
    const exitCode = await listUserDB(DBOSCloudHost, options.json)
    process.exit(exitCode);
  }))

userdbCommands
  .command('delete')
  .argument('<string>', 'database name')
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
  .command('init')
  .description('Initialize the Monitoring Dashboard')
  .action(async () => {
    const exitCode = await initDashboard(DBOSCloudHost);
    process.exit(exitCode);
  });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
