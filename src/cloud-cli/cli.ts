#!/usr/bin/env node

import { deploy } from "./deploy";
import { Command } from 'commander';
import { login } from "./login";

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
  .option('-n, --name <string>', 'Specify the app name')
  .option('-h, --host <string>', 'Specify the host', 'localhost')
  .action(async (options: { name: string, host: string }) => {
    if (!options.name) {
      console.error('Error: the --name option is required.');
      return;
    }
    await deploy(options.name, options.host);
  });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
