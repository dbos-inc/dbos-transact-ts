#!/usr/bin/env node

import { dbosConfigFilePath, parseConfigFile } from "./config";
import { DBOSRuntime, DBOSRuntimeConfig } from "./runtime";

import { Command } from 'commander';
import { DBOSConfig } from "../dbos-executor";
import { init } from "./init";
import { debugWorkflow } from "./debug";
import { migrate, rollbackMigration } from "./migrate";

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

interface DBOSDebugOptions extends DBOSCLIStartOptions {
  proxy: string, // TODO: in the future, we provide the proxy URL
  uuid: string, // Workflow UUID
}

program
  .command('start')
  .description('Start the server')
  .option('-p, --port <number>', 'Specify the port number')
  .option('-l, --loglevel <string>', 'Specify log level')
  .option('-c, --configfile <string>', 'Specify the config file path', dbosConfigFilePath)
  .option('-e, --entrypoint <string>', 'Specify the entrypoint file path')
  .action(async (options: DBOSCLIStartOptions) => {
    const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(options);
    await using runtime = new DBOSRuntime(dbosConfig, runtimeConfig);
    {
      await runtime.init();
      runtime.startServer();
    }
  }
  );

program
  .command('debug')
  .description('Debug a workflow')
  .requiredOption('-x, --proxy <string>', 'Specify the debugger proxy URL')
  .requiredOption('-u, --uuid <string>', 'Specify the workflow UUID to debug')
  .option('-l, --loglevel <string>', 'Specify log level')
  .option('-c, --configfile <string>', 'Specify the config file path', dbosConfigFilePath)
  .option('-e, --entrypoint <string>', 'Specify the entrypoint file path')
  .action(async (options: DBOSDebugOptions) => {
    const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(options);
    await debugWorkflow(dbosConfig, runtimeConfig, options.proxy, options.uuid);
  });

program
  .command('init')
  .description('Init a DBOS application')
  .option('-n, --appName <application-name>', 'Application name', 'dbos-hello-app')
  .action(async (options: { appName: string }) => {
    await init(options.appName);
  });

program
  .command('migrate')
  .description("Perform a database migration")
  .action((async () => {
    const exitCode = await migrate();
    process.exit(exitCode);
  }))

program
  .command('rollback')
  .action((() => {
    const exitCode = rollbackMigration();
    process.exit(exitCode);
  }))

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
