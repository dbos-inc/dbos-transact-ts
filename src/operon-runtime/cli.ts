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
    const runtime = new OperonRuntime();
    await runtime.startServer(port);
  });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
