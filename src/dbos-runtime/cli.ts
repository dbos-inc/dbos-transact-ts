#!/usr/bin/env node

import { dbosConfigFilePath, parseConfigFile } from "./config";
import { DBOSRuntime, DBOSRuntimeConfig } from "./runtime";
import { Command } from 'commander';
import { DBOSConfig } from "../dbos-sdk";
import { init } from "./init";
import { generateOpenApi } from "./openApi";
import YAML from 'yaml';
import fs from 'node:fs/promises';
import path from 'node:path';

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

export interface DBOSCLIStartOptions {
  port?: number,
  loglevel?: string,
  configfile?: string,
  entrypoint?: string,
}

program
  .command("openapi")
  .argument('<entrypoint>', 'Specify the entrypoint file path')
  .action(async (entrypoint: string) => {
    const openapi = await generateOpenApi(entrypoint);
    if (openapi) {
      const filename = path.join(path.dirname(entrypoint), "openapi.yaml");
      const yaml = `# OpenApi specification generated for application\n\n` + YAML.stringify(openapi, { aliasDuplicateObjects: false });
      await fs.writeFile(filename, yaml, { encoding: 'utf-8' });
    }
  });

program
  .command('start')
  .description('Start the server')
  .option('-p, --port <number>', 'Specify the port number')
  .option('-l, --loglevel <string>', 'Specify log level')
  .option('-c, --configfile <string>', 'Specify the config file path', dbosConfigFilePath)
  .option('-e, --entrypoint <string>', 'Specify the entrypoint file path')
  .action(async (options: DBOSCLIStartOptions) => {
    const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(options);
    const runtime = new DBOSRuntime(dbosConfig, runtimeConfig);
    await runtime.init();
    runtime.startServer();
  });

program
  .command('init')
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
