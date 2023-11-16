#!/usr/bin/env node

import {
  registerApp,
  updateApp,
  listApps,
  deleteApp,
  deployAppCode,
  getAppLogs,
} from "./applications/";
import { Command } from 'commander';
import { login } from "./login";
import { registerUser } from "./register";
import { createUserDb, getUserDb, deleteUserDb } from "./userdb";

const program = new Command();

const DEFAULT_HOST = "localhost"
const DEFAULT_PORT = "8080"

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageJson = require('../../../package.json') as { version: string };
program.
  version(packageJson.version);

/////////////////////
/* AUTHENTICATION  */
/////////////////////

program
  .command('login')
  .description('Log in Operon cloud')
  .requiredOption('-u, --userName <string>', 'User name for login')
  .action((options: { userName: string }) => {
    login(options.userName);
  });

program
  .command('register')
  .description('Register a user and log in Operon cloud')
  .requiredOption('-u, --userName <string>', 'User name')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <string>', 'Specify the port', DEFAULT_PORT)
  .action(async (options: { userName: string, host: string, port: string }) => {
    const success = await registerUser(options.userName, options.host, options.port);
    // Then, log in as the user.
    if (success) {
      login(options.userName);
    }
  });

/////////////////////////////
/* APPLICATIONS MANAGEMENT */
/////////////////////////////

const applicationCommands = program
  .command('applications')
  .description('Manage your DBOS applications')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <string>', 'Specify the port', DEFAULT_PORT)

applicationCommands
  .command('register')
  .description('Register a new application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .option('-m, --machines <string>', 'Number of VMs to deploy', '1')
  .action(async (options: { name: string, machines: string }) => {
    const { host, port }: { host: string, port: string } = applicationCommands.opts()
    await registerApp(options.name, host, port, parseInt(options.machines));
  });

applicationCommands
  .command('update')
  .description('Update an application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .requiredOption('-m, --machines <string>', 'Number of VMs to deploy')
  .action(async (options: { name: string, machines: string }) => {
    const { host, port }: { host: string, port: string } = applicationCommands.opts()
    await updateApp(options.name, host, port, parseInt(options.machines));
  });

applicationCommands
  .command('deploy')
  .description('Deploy an application code to the cloud')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .action(async (options: { name: string }) => {
    const { host, port }: { host: string, port: string } = applicationCommands.opts()
    await deployAppCode(options.name, host, port);
  });

applicationCommands
  .command('delete')
  .description('Delete a previously deployed application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .action(async (options: { name: string }) => {
    const { host, port }: { host: string, port: string } = applicationCommands.opts()
    await deleteApp(options.name, host, port);
  });

applicationCommands
  .command('list')
  .description('List all deployed applications')
  .action(async () => {
    const { host, port }: { host: string, port: string } = applicationCommands.opts()
    await listApps(host, port);
  });

applicationCommands
  .command('logs')
  .description('Print the microVM logs of a deployed application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .action(async (options: { name: string }) => {
    const { host, port }: { host: string, port: string } = applicationCommands.opts()
    await getAppLogs(options.name, host, port);
  });

//////////////////////////////
/* USER DATABASE MANAGEMENT */
//////////////////////////////

const userdb = program
  .command('userdb')
  .description('Manage your databases')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <string>', 'Specify the port', DEFAULT_PORT)

userdb
  .command('create')
  .argument('<string>', 'database name')
  .option('-a, --admin <string>', 'Specify the admin user', 'postgres')
  .option('-W, --password <string>', 'Specify the admin password', 'postgres')
  .option('-s, --sync <bool>', 'make synchronous call', false)
  .action((async (dbname: string, options: { admin: string, password: string, sync: boolean }) => {
    const { host, port }: { host: string, port: string } = applicationCommands.opts()
    await createUserDb(host, port, dbname, options.admin, options.password, options.sync)
  }))

userdb
  .command('status')
  .argument('<string>', 'database name')
  .action((async (dbname: string) => {
    const { host, port }: { host: string, port: string } = applicationCommands.opts()
    await getUserDb(host, port, dbname)
  }))

userdb
  .command('delete')
  .argument('<string>', 'database name')
  .option('-s, --sync <bool>', 'make synchronous call', false)
  .action((async (dbname: string, options: { sync:boolean }) => {
    const { host, port }: { host: string, port: string } = applicationCommands.opts()
    await deleteUserDb(host, port, dbname, options.sync)
  }))

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
