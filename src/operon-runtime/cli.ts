#!/usr/bin/env node

import { deploy } from "./deploy";
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
  .option('-n, --name <type>', 'Specify the app name')
  .option('-h, --host <type>', 'Specify the host', 'localhost')
  .action(async (options: { name: string, host: string }) => {
    if (!options.name) {
      console.error('Error: the --name option is required.');
      return;
    }
    deploy(options.name, options.host);
  });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
