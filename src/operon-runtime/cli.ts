#!/usr/bin/env node

import { operonConfigFilePath, parseConfigFile } from "./config";
import { OperonRuntime, OperonRuntimeConfig } from "./runtime";
import { Command } from 'commander';
import { OperonConfig } from "../operon";
import { init } from "./init";
import * as ts from 'typescript';
import { generateOpenApi } from "./openApi";
import YAML from 'yaml';
import fs from 'node:fs/promises';
import path from 'node:path';

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

export interface OperonCLIStartOptions {
  port?: number,
  loglevel?: string,
  configfile?: string,
  entrypoint?: string,
}

program
  .command("generate")
  .requiredOption('-e, --entrypoint <string>', 'Specify the entrypoint file path')
  .action(async ({ entrypoint }: { entrypoint: string }) => {
    const program = ts.createProgram([entrypoint], {});
    const openapi = generateOpenApi(program);

    const filename = path.join(path.dirname(entrypoint), "swagger.yaml");
    const yaml = YAML.stringify(openapi);
    await fs.writeFile(filename, yaml, { encoding: 'utf-8' });
  });

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
