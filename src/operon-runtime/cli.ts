#!/usr/bin/env node

import { OperonRuntime } from "./runtime";
import { Command } from 'commander';

const program = new Command();

program
  .command('start')
  .description('Start the server')
  .option('-p, --port <type>', 'Specify the port number', '3000')
  .action(async (options: { port: string }) => {
    const port = parseInt(options.port);
    console.log(`Starting server on port: ${port}`);
    const runtime = new OperonRuntime();
    await runtime.startServer(port);
  });

program
  .command('deploy')
  .description('Deploy an application')
  .option('-h, --host <type>', 'Specify the host', 'localhost')
  .action(async (options: { host: string }) => {
    console.log(`DEPLOY ${options.host}`);
  });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
