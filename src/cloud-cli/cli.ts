#!/usr/bin/env node

import { deploy } from "./deploy";
import { Command } from 'commander';
import { login } from "./login";
import { registerUser } from "./register";
import { deleteApp } from "./delete";
import { getAppLogs } from "./monitor";
import { createUserDb, getUserDb, deleteUserDb } from "./userdb";

const program = new Command();

const DEFAULT_HOST = "localhost"
const DEFAULT_PORT = "8080"

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageJson = require('../../../package.json') as { version: string };
program.
  version(packageJson.version);

///////////////////////
/* CLOUD DEPLOYMENT  */
///////////////////////

program
  .command('login')
  .description('Log in Operon cloud')
  .requiredOption('-u, --userName <string>', 'User name for login', )
  .action((options: { userName: string }) => {
    login(options.userName);
  });

program
  .command('deploy')
  .description('Deploy an application to the cloud')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <port>', 'Specify the port', DEFAULT_PORT)
  .option('-m, --machines <number>', 'Number of VMs to deploy', '1')
  .action(async (options: { name: string, host: string, port: string, machines: string }) => {
    await deploy(options.name, options.host, options.port, parseInt(options.machines));
  });

program
  .command('register')
  .description('Register a user and log in Operon cloud')
  .requiredOption('-u, --userName <string>', 'User name', )
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <port>', 'Specify the port', DEFAULT_PORT)
  .action(async (options: { userName: string, host: string, port: string }) => {
    const success = await registerUser(options.userName, options.host, options.port);
    // Then, log in as the user.
    if (success) {
      login(options.userName);
    }
  });

program
  .command('delete')
  .description('Delete a previously deployed application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <port>', 'Specify the port', DEFAULT_PORT)
  .action(async (options: { name: string, host: string, port: string }) => {
    await deleteApp(options.name, options.host, options.port);
  });

program
  .command('logs')
  .description('Print the microVM logs of a deployed application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <port>', 'Specify the port', DEFAULT_PORT)
  .action(async (options: { name: string, host: string, port: string }) => {
    await getAppLogs(options.name, options.host, options.port);
  });

const userdb = program
  .command('userdb')
  
userdb
  .command('create')
  .argument('<string>', 'database name')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <port>', 'Specify the port', DEFAULT_PORT)
  .option('-a, --admin <admin>', 'Specify the admin user', 'postgres')
  .option('-W, --password <admin>', 'Specify the admin password', 'postgres')
  .option('-s, --sync', 'make synchronous call', false)
  .action((async (dbname: string, options: { host: string, port: string, admin: string, password: string, sync: boolean }) => {
    await createUserDb(options.host, options.port, dbname, options.admin, options.password, options.sync)
  }))

userdb
  .command('status')
  .argument('<string>', 'database name')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <port>', 'Specify the port', DEFAULT_PORT)
  .action((async (dbname: string, options: { host: string, port: string }) => {
    await getUserDb(options.host, options.port, dbname)
  })) 

userdb
  .command('delete')
  .argument('<string>', 'database name')
  .option('-h, --host <string>', 'Specify the host', DEFAULT_HOST)
  .option('-p, --port <port>', 'Specify the port', DEFAULT_PORT)
  .option('-s, --sync', 'make synchronous call', false)
  .action((async (dbname: string, options: { host: string, port: string, sync:boolean }) => {
    await deleteUserDb(options.host, options.port, dbname, options.sync)
  })) 

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
