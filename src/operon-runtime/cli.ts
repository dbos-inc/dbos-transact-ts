#!/usr/bin/env node

import { operonConfigFilePath, parseConfigFile } from "./config";
import { OperonRuntime, OperonRuntimeConfig } from "./runtime";
import { Command } from 'commander';
import { OperonConfig } from "../operon";
import { init } from "./init";

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

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
