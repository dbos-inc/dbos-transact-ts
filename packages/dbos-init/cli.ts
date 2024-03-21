#!/usr/bin/env node
import { Command } from 'commander';
import { init } from './init.js';
import fs from 'fs'
import path from "path";
import { Package } from "update-notifier";

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

// eslint-disable-next-line @typescript-eslint/no-var-requires
import { fileURLToPath } from 'url';
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const packageJson = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "package.json")).toString()) as Package;
program.version(packageJson.version);

program
  .description('Init a DBOS application')
  .option('-n, --appName <application-name>', 'Application name', 'dbos-hello-app')
  .action(async (options: { appName: string }) => {
    await init(options.appName);
  });

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
else {
  program.parse(process.argv);
}

