#!/usr/bin/env node

import { operonConfigFilePath, parseConfigFile } from "./config";
import { deploy } from "./deploy";
import { OperonRuntime, OperonRuntimeConfig } from "./runtime";
import { Command } from 'commander';
import { OperonConfig } from "../operon";
import { init } from "./init";
import { login } from "./login";

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageJson = require('../../../package.json') as { version: string };
program.
  version(packageJson.version);

export interface OperonCLIStartOptions {
  port?: number,
  loglevel?: string,
  configfile?: string,
  entrypoint?: string,
}

program
  .command('start')
  .description('Start the server')
  .option('-p, --port <number>', 'Specify the port number')
  .option('-l, --loglevel <string>', 'Specify Operon log level')
  .option('-c, --configfile <string>', 'Specify the Operon config file path', operonConfigFilePath)
  .option('-e, --entrypoint <string>', 'Specify the entrypoint file path')
  .action(async (options: OperonCLIStartOptions) => {
    const [operonConfig, runtimeConfig]: [OperonConfig, OperonRuntimeConfig] = parseConfigFile(options);
    const runtime = new OperonRuntime(operonConfig, runtimeConfig);
    await runtime.init();
    runtime.startServer();
  });

program
  .command('init')
  .description('Init an Operon application')
  .option('-n, --appName <application-name>', 'Application name', 'operon-hello-app')
  .action(async (options: { appName: string }) => {
    await init(options.appName);
  });

///////////////////////
/* CLOUD DEPLOYMENT  */
///////////////////////

program
  .command('login')
  .description('Log in Operon cloud')
  .requiredOption('-u, --userName <string>', 'User name for login', )
  .action(async (options: { userName: string }) => {
    await login(options.userName);
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
