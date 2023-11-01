#!/usr/bin/env node

import { deploy } from "./deploy";
import { Command } from 'commander';
import { login } from "./login";
import { registerUser } from "./register";
import { deleteApp } from "./delete";
import { getAppLogs } from "./monitor";

const program = new Command();

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
  .option('-h, --host <string>', 'Specify the host', 'localhost')
  .action(async (options: { name: string, host: string }) => {
    await deploy(options.name, options.host);
  });

program
  .command('register')
  .description('Register a user and log in Operon cloud')
  .requiredOption('-u, --userName <string>', 'User name', )
  .option('-h, --host <string>', 'Specify the host', 'localhost')
  .action(async (options: { userName: string, host: string }) => {
    const success = await registerUser(options.userName, options.host);
    // Then, log in as the user.
    if (success) {
      login(options.userName);
    }
  });

program
  .command('delete')
  .description('Delete a previously deployed application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .option('-h, --host <string>', 'Specify the host', 'localhost')
  .action(async (options: { name: string, host: string }) => {
    await deleteApp(options.name, options.host);
  });

program
  .command('logs')
  .description('Print the microVM logs of a deployed application')
  .requiredOption('-n, --name <string>', 'Specify the app name')
  .option('-h, --host <string>', 'Specify the host', 'localhost')
  .action(async (options: { name: string, host: string }) => {
    await getAppLogs(options.name, options.host);
  });


program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
