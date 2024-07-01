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
import { cancelWorkflow, getWorkflow, listWorkflows, reattemptWorkflow } from './workflow_management';
import { GetWorkflowsInput, StatusString } from '..';
import { exit } from 'node:process';

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

export interface DBOSCLIStartOptions {
  port?: number;
  loglevel?: string;
  configfile?: string;
  appDir?: string;
  appVersion?: string | boolean;
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
  appVersion?: string | boolean;
}

// eslint-disable-next-line @typescript-eslint/no-require-imports
const packageJson = require("../../../package.json") as { version: string };
program.version(packageJson.version);

program
  .command("start")
  .description("Start the server")
  .option("-p, --port <number>", "Specify the port number")
  .option("-l, --loglevel <string>", "Specify log level")
  .option("-c, --configfile <string>", "Specify the config file path (DEPRECATED)")
  .option("-d, --appDir <string>", "Specify the application root directory")
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .action(async (options: DBOSCLIStartOptions) => {
    if (options?.configfile) {
      console.warn('\x1b[33m%s\x1b[0m', "The --configfile option is deprecated. Please use --appDir instead.");
    }
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
  .option("-c, --configfile <string>", "Specify the config file path (DEPRECATED)")
  .option("-d, --appDir <string>", "Specify the application root directory")
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
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

/////////////////////////
/* WORKFLOW MANAGEMENT */
/////////////////////////

const workflowCommands = program.command("workflow").alias("workflows").alias("wf").description("Manage DBOS workflows");

workflowCommands
  .command('list')
  .description('List workflows from your application')
  .option('-l, --limit <number>', 'Limit the results returned', "10")
  .option('-u, --user <string>', 'Retrieve workflows run by this user')
  .option('-s, --start-time <string>', 'Retrieve workflows starting after this timestamp (ISO 8601 format)')
  .option('-e, --end-time <string>', 'Retrieve workflows starting before this timestamp (ISO 8601 format)')
  .option('-S, --status <string>', 'Retrieve workflows with this status (PENDING, SUCCESS, ERROR, RETRIES_EXCEEDED, or CANCELLED)')
  .option('-v, --application-version <string>', 'Retrieve workflows with this application version')
  .option('--request', 'Retrieve workflow request information')
  .option("-d, --appDir <string>", "Specify the application root directory")
  .action(async (options: { limit?: string, appDir?: string, user?: string, startTime?: string, endTime?: string, status?: string, applicationVersion?: string, request: boolean }) => {
    const [dbosConfig, _] = parseConfigFile(options);
    if (options.status && !Object.values(StatusString).includes(options.status as typeof StatusString[keyof typeof StatusString])) {
      console.error("Invalid status: ", options.status);
      exit(1);
    }
    const input: GetWorkflowsInput = {
      limit: Number(options.limit),
      authenticatedUser: options.user,
      startTime: options.startTime,
      endTime: options.endTime,
      status: options.status as typeof StatusString[keyof typeof StatusString],
      applicationVersion: options.applicationVersion,
    }
    const output = await listWorkflows(dbosConfig, input, options.request);
    console.log(JSON.stringify(output))
  });

workflowCommands
  .command('get')
  .description('Retrieve the status of a workflow')
  .argument("<uuid>", "Target workflow UUID")
  .option("-d, --appDir <string>", "Specify the application root directory")
  .option('--request', 'Retrieve workflow request information')
  .action(async (uuid: string, options: { appDir?: string, request: boolean }) => {
    const [dbosConfig, _] = parseConfigFile(options);
    const output = await getWorkflow(dbosConfig, uuid, options.request);
    console.log(JSON.stringify(output))
  });

workflowCommands
  .command('cancel')
  .description('Cancel a workflow so it is no longer automatically retried or restarted')
  .argument("<uuid>", "Target workflow UUID")
  .option("-d, --appDir <string>", "Specify the application root directory")
  .action(async (uuid: string, options: { appDir?: string }) => {
    const [dbosConfig, _] = parseConfigFile(options);
    await cancelWorkflow(dbosConfig, uuid);
  });

workflowCommands
  .command('resume')
  .description('Resume a workflow from the last step it executed, keeping its UUID')
  .argument("<uuid>", "Target workflow UUID")
  .option("-d, --appDir <string>", "Specify the application root directory")
  .action(async (uuid: string, options: { appDir?: string }) => {
    const [dbosConfig, runtimeConfig] = parseConfigFile(options);
    const output = await reattemptWorkflow(dbosConfig, runtimeConfig, uuid, false);
    console.log(`Workflow output: ${JSON.stringify(output)}`);
  });

workflowCommands
  .command('restart')
  .description('Restart a workflow from the beginning with a new UUID')
  .argument("<uuid>", "Target workflow UUID")
  .option("-d, --appDir <string>", "Specify the application root directory")
  .action(async (uuid: string, options: { appDir?: string }) => {
    const [dbosConfig, runtimeConfig] = parseConfigFile(options);
    const output = await reattemptWorkflow(dbosConfig, runtimeConfig, uuid, true);
    console.log(`Workflow output: ${JSON.stringify(output)}`);
  });

/////////////
/* PARSING */
/////////////

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}

//Takes an action function(configFile, logger) that returns a numeric exit code.
//If otel exporter is specified in configFile, adds it to the logger and flushes it after.
//If action throws, logs the exception and sets the exit code to 1.
//Finally, terminates the program with the exit code.
export async function runAndLog(action: (configFile: ConfigFile, logger: GlobalLogger) => Promise<number> | number) {
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
