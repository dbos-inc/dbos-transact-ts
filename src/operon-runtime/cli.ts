#!/usr/bin/env node

import { parseConfigFile } from "./config";
import { deploy } from "./deploy";
import { OperonRuntime, OperonRuntimeConfig } from "./runtime";
import { Command } from 'commander';
import { OperonConfig } from "../operon";
import { init } from "./init";

const program = new Command();

export interface OperonCLIOptions {
  port: number,
  loglevel: string,
}

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageJson = require('../../../package.json') as { version: string };
program.
  version(packageJson.version);

program
  .command('start')
  .description('Start the server')
  .option('-p, --port <number>', 'Specify the port number')
  .option('-l, --loglevel <string>', 'Specify Operon log level', 'info')
  .action(async (options: OperonCLIOptions) => {
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
