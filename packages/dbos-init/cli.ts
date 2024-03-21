#!/usr/bin/env node
import { Command } from 'commander';
import { init } from './init.js';

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageJson = require('./package.json') as { version: string };
program.version(packageJson.version);

program
  .description('Init a DBOS application')
  .option('-n, --appName <application-name>', 'Application name', 'dbos-hello-app')
  .action(async (options: { appName: string }) => {
    await init(options.appName);
  });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}