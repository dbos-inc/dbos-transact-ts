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
import { createUserDb, getUserDb, deleteUserDb, migrate, rollbackMigration } from "./userdb";
import { credentialsExist } from "./utils";

const program = new Command();

const DEFAULT_HOST = process.env.DBOS_DOMAIN;

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
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .action(async (options: { username: string, host: string}) => {
    if (!credentialsExist()) {
      const exitCode = await login(options.username);
      if (exitCode !== 0) {
        process.exit(exitCode);
      }
    }
    const exitCode = await registerUser(options.username, options.host);
    process.exit(exitCode);
  });

/////////////////////////////
/* APPLICATIONS MANAGEMENT */
/////////////////////////////

const applicationCommands = program
  .command('applications')
  .description('Manage your DBOS applications')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)

applicationCommands
  .command('register')
  .description('Register a new application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .requiredOption('-d, --database <string>', 'Specify the app database name')
  .option('-m, --machines <string>', 'Number of VMs to deploy', '1')
  .action(async (options: { name: string, database: string, machines: string }) => {
    const { host }: { host: string } = applicationCommands.opts()
    const exitCode = await registerApp(options.name, options.database, host, parseInt(options.machines));
    process.exit(exitCode);
  });

applicationCommands
  .command('update')
  .description('Update an application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .requiredOption('-m, --machines <string>', 'Number of VMs to deploy')
  .action(async (options: { name: string, machines: string }) => {
    const { host }: { host: string } = applicationCommands.opts()
    const exitCode = await updateApp(options.name, host, parseInt(options.machines));
    process.exit(exitCode);
  });

applicationCommands
  .command('deploy')
  .description('Deploy an application code to the cloud')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .action(async (options: { name: string }) => {
    const { host }: { host: string } = applicationCommands.opts()
    const exitCode = await deployAppCode(options.name, host);
    process.exit(exitCode);
  });

applicationCommands
  .command('delete')
  .description('Delete a previously deployed application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .action(async (options: { name: string }) => {
    const { host }: { host: string } = applicationCommands.opts()
    const exitCode = await deleteApp(options.name, host);
    process.exit(exitCode);
  });

applicationCommands
  .command('list')
  .description('List all deployed applications')
  .action(async () => {
    const { host }: { host: string } = applicationCommands.opts()
    const exitCode = await listApps(host);
    process.exit(exitCode);
  });

applicationCommands
  .command('logs')
  .description('Print the microVM logs of a deployed application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .action(async (options: { name: string }) => {
    const { host }: { host: string } = applicationCommands.opts()
    const exitCode = await getAppLogs(options.name, host);
    process.exit(exitCode);
  });

//////////////////////////////
/* USER DATABASE MANAGEMENT */
//////////////////////////////

const userdbCommands = program
  .command('userdb')
  .description('Manage your databases')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)

userdbCommands
  .command('create')
  .argument('<string>', 'database name')
  .requiredOption('-a, --admin <string>', 'Specify the admin user')
  .requiredOption('-W, --password <string>', 'Specify the admin password')
  .option('-s, --sync', 'make synchronous call', true)
  .action((async (dbname: string, options: { admin: string, password: string, sync: boolean }) => {
    const { host }: { host: string } = userdbCommands.opts()
    await createUserDb(host, dbname, options.admin, options.password, options.sync)
  }))

userdbCommands
  .command('status')
  .argument('<string>', 'database name')
  .action((async (dbname: string) => {
    const { host }: { host: string } = userdbCommands.opts()
    await getUserDb(host, dbname)
  }))

userdbCommands
  .command('delete')
  .argument('<string>', 'database name')
  .action((async (dbname: string) => {
    const { host }: { host: string } = userdbCommands.opts()
    await deleteUserDb(host, dbname)
  }))

userdbCommands
  .command('migrate')
  .action((() => {
    const exitCode = migrate();
    process.exit(exitCode);
  }))

userdbCommands
  .command('rollbackmigration')
  .action((() => {
    const exitCode = rollbackMigration();
    process.exit(exitCode);
  }))

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
