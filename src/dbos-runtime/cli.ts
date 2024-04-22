#!/usr/bin/env node
import { DBOSRuntime, DBOSRuntimeConfig } from './runtime';
import { ConfigFile, dbosConfigFilePath, loadConfigFile, parseConfigFile } from './config';
import { Command } from 'commander';
import { DBOSConfig } from '../dbos-executor';
import { debugWorkflow } from './debug';
import { migrate, rollbackMigration } from './migrate';
import { GlobalLogger } from '../telemetry/logs';
import { TelemetryCollector } from '../telemetry/collector';
import { TelemetryExporter } from '../telemetry/exporters';
import { configure } from './configure';

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

export interface DBOSCLIStartOptions {
  port?: number;
  loglevel?: string;
  configfile?: string;
}

export interface DBOSConfigureOptions {
  host?: string;
  port?: number;
  username?: string;
}

interface DBOSDebugOptions {
  uuid: string; // Workflow UUID
  proxy: string;
  loglevel?: string;
  configfile?: string;
}

// eslint-disable-next-line @typescript-eslint/no-var-requires
const packageJson = require("../../../package.json") as { version: string };
program.version(packageJson.version);

program
  .command("start")
  .description("Start the server")
  .option("-p, --port <number>", "Specify the port number")
  .option("-l, --loglevel <string>", "Specify log level")
  .option("-c, --configfile <string>", "Specify the config file path", dbosConfigFilePath)
  .action(async (options: DBOSCLIStartOptions) => {
    const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(options);
    const runtime = new DBOSRuntime(dbosConfig, runtimeConfig);
    await runtime.initAndStart();
  });

program
  .command("debug")
  .description("Debug a workflow")
  .option("-x, --proxy <string>", "Specify the time-travel debug proxy URL for debugging cloud traces")
  .requiredOption("-u, --uuid <string>", "Specify the workflow UUID to replay")
  .option("-l, --loglevel <string>", "Specify log level")
  .option("-c, --configfile <string>", "Specify the config file path", dbosConfigFilePath)
  .action(async (options: DBOSDebugOptions) => {
    const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(options, options.proxy !== undefined);
    await debugWorkflow(dbosConfig, runtimeConfig, options.uuid, options.proxy);
  });

program
  .command('init')
  .description('Init a DBOS application')
  .option('-n, --appName <application-name>', 'Application name', 'dbos-hello-app')
  .action((_options: { appName: string }) => {
    console.log("NOTE: This command has been removed in favor of `npx @dbos-inc/create` or `npm create @dbos-inc`");
  });

program
  .command('configure')
  .alias('config')
  .option('-h, --host <string>', 'Specify your Postgres server hostname')
  .option('-p, --port <number>', 'Specify your Postgres server port')
  .option('-U, --username <number>', 'Specify your Postgres username')
  .action(async (options: DBOSConfigureOptions) => {
    await configure(options.host, options.port, options.username);
  });

program
  .command('migrate')
  .description("Perform a database migration")
  .action(async () => { await runAndLog(migrate); });

program
  .command('rollback')
  .action(async () => { await runAndLog(rollbackMigration); });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}

//Takes an action function(configFile, logger) that returns a numeric exit code.
//If otel exporter is specified in configFile, adds it to the logger and flushes it after.
//If action throws, logs the exception and sets the exit code to 1.
//Finally, terminates the program with the exit code.
export async function runAndLog(action: (configFile: ConfigFile, logger: GlobalLogger) => Promise<number>) {
  let logger = new GlobalLogger();
  const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
  if (!configFile) {
    logger.error(`Failed to parse ${dbosConfigFilePath}`);
    process.exit(1);
  }
  let terminate = undefined;
  if (configFile.telemetry?.OTLPExporter) {
    logger = new GlobalLogger(new TelemetryCollector(new TelemetryExporter(configFile.telemetry.OTLPExporter)), configFile.telemetry?.logs);
    terminate = (code: number) => {
      void logger.destroy().finally(() => {
        process.exit(code);
      });
    };
  } else {
    terminate = (code: number) => {
      process.exit(code);
    };
  }
  let returnCode = 1;
  try {
    returnCode = await action(configFile, logger);
  } catch (e) {
    logger.error(e);
  }
  terminate(returnCode);
}
