#!/usr/bin/env node
import { DBOSRuntime, DBOSRuntimeConfig } from './runtime';
import { ConfigFile, dbosConfigFilePath, getDatbaseConfig, readConfigFile, writeConfigFile } from './config';
import { Command } from 'commander';
import { DBOSConfigInternal } from '../dbos-executor';
import { debugWorkflow } from './debug';
import { migrate, rollbackMigration } from './migrate';
import { GlobalLogger } from '../telemetry/logs';
import { TelemetryCollector } from '../telemetry/collector';
import { TelemetryExporter } from '../telemetry/exporters';
import { DBOSClient, StatusString } from '..';
import { exit } from 'node:process';
import { runCommand } from './commands';
import { reset } from './reset';
import { startDockerPg, stopDockerPg } from './docker_pg_helper';
import { input, confirm } from '@inquirer/prompts';

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
  silent?: boolean;
}

export interface DBOSConfigureOptions {
  host?: string;
  port?: number;
  username?: string;
}

interface DBOSDebugOptions {
  uuid: string; // Workflow UUID
  proxy?: string; // deprecated
  loglevel?: string;
  configfile?: string;
  appVersion?: string | boolean;
}

// eslint-disable-next-line @typescript-eslint/no-require-imports
const packageJson = require('../../../package.json') as { version: string };
program.version(packageJson.version);

program
  .command('start')
  .description('Start the server')
  .option('-p, --port <number>', 'Specify the port number')
  .option('-l, --loglevel <string>', 'Specify log level')
  .option('-c, --configfile <string>', 'Specify the config file path (DEPRECATED)')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .action(async (options: DBOSCLIStartOptions) => {
    if (options?.configfile) {
      console.warn('\x1b[33m%s\x1b[0m', 'The --configfile option is deprecated. Please use --appDir instead.');
    }
    options.silent = true;
    const [dbosConfig, runtimeConfig]: [DBOSConfigInternal, DBOSRuntimeConfig] = parseConfigFile(options);
    // If no start commands are provided, start the DBOS runtime
    if (runtimeConfig.start.length === 0) {
      const runtime = new DBOSRuntime(dbosConfig, runtimeConfig);
      await runtime.initAndStart();
    } else {
      const logger = getGlobalLogger(dbosConfig);
      for (const command of runtimeConfig.start) {
        try {
          const ret = await runCommand(command, logger, options.appDir);
          if (ret !== 0) {
            process.exit(ret);
          }
        } catch (e) {
          // We always reject the command with a return code
          process.exit(e as number);
        }
      }
    }
  });

program
  .command('debug')
  .description('Debug a workflow')
  .requiredOption('-u, --uuid <string>', 'Specify the workflow UUID to replay')
  .option('-l, --loglevel <string>', 'Specify log level')
  .option('-c, --configfile <string>', 'Specify the config file path (DEPRECATED)')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .action(async (options: DBOSDebugOptions) => {
    const [dbosConfig, runtimeConfig]: [DBOSConfigInternal, DBOSRuntimeConfig] = parseConfigFile({
      ...options,
      forceConsole: true,
    });
    await debugWorkflow(dbosConfig, runtimeConfig, options.uuid);
  });

program
  .command('init')
  .description('Init a DBOS application')
  .option('-n, --appName <application-name>', 'Application name', 'dbos-hello-app')
  .action((_options: { appName: string }) => {
    console.log('NOTE: This command has been removed in favor of `npx @dbos-inc/create` or `npm create @dbos-inc`');
  });

program
  .command('configure')
  .alias('config')
  .option('-h, --host <string>', 'Specify your Postgres server hostname')
  .option('-p, --port <number>', 'Specify your Postgres server port')
  .option('-U, --username <number>', 'Specify your Postgres username')
  .option('-d, --appDir <string>', 'Specify the application root directory')

  .action(async (options: { host?: string; port?: number; username?: string; appDir?: string }) => {
    const config = await readConfigFile(options.appDir);
    if (config.database_url) {
      console.log(`Database is already configured as ${config.database_url}`);
      const proceed = await confirm({
        message: 'Do you want to re-configure the database connection?',
        default: false,
      });
      if (!proceed) {
        console.log('Aborting configuration.');
        return;
      }
    }

    const host =
      options.host ??
      (await input({
        message: 'What is the hostname of your Postgres server?',
        default: 'localhost',
      }));
    const port =
      options.port ??
      (await input({
        message: 'What is the port of your Postgres server?',
        default: '5432',
      }));
    const username =
      options.username ??
      (await input({
        message: 'What is your Postgres username?',
        default: 'postgres',
      }));

    config.database_url = `postgresql://${username}@${host}:${port}/${config.name}`;

    await writeConfigFile(config, options.appDir);
  });

program
  .command('migrate')
  .description('Perform a database migration')
  .action(async () => {
    await runAndLog(migrate);
  });

program
  .command('postgres')
  .description('Helps you setting up a local Postgres database with Docker')
  .addCommand(
    new Command('start').description('Start a local Postgres database with Docker').action(async () => {
      await startDockerPg();
    }),
  )
  .addCommand(
    new Command('stop').description('Stop the local Postgres database with Docker').action(async () => {
      await stopDockerPg();
    }),
  );

program
  .command('reset')
  .description('reset the system database')
  .option('-y, --yes', 'Skip confirmation prompt', false)
  .action(async (options: { yes: boolean }) => {
    const logger = new GlobalLogger();
    const [config] = parseConfigFile();
    await reset(config, logger, options.yes);
  });

program.command('rollback').action(async () => {
  await runAndLog(rollbackMigration);
});

/////////////////////////
/* WORKFLOW MANAGEMENT */
/////////////////////////

const workflowCommands = program
  .command('workflow')
  .alias('workflows')
  .alias('wf')
  .description('Manage DBOS workflows');

workflowCommands
  .command('list')
  .description('List workflows from your application')
  .option('-n, --name <string>', 'Retrieve functions with this name')
  .option('-l, --limit <number>', 'Limit the results returned', '10')
  .option('-u, --user <string>', 'Retrieve workflows run by this user')
  .option('-s, --start-time <string>', 'Retrieve workflows starting after this timestamp (ISO 8601 format)')
  .option('-e, --end-time <string>', 'Retrieve workflows starting before this timestamp (ISO 8601 format)')
  .option(
    '-S, --status <string>',
    'Retrieve workflows with this status (PENDING, SUCCESS, ERROR, RETRIES_EXCEEDED, ENQUEUED, or CANCELLED)',
  )
  .option('-v, --application-version <string>', 'Retrieve workflows with this application version')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(
    async (options: {
      name?: string;
      limit?: string;
      appDir?: string;
      user?: string;
      startTime?: string;
      endTime?: string;
      status?: string;
      applicationVersion?: string;
    }) => {
      if (
        options.status &&
        !Object.values(StatusString).includes(options.status as (typeof StatusString)[keyof typeof StatusString])
      ) {
        console.error('Invalid status: ', options.status);
        exit(1);
      }

      const configFile = await readConfigFile(options.appDir);
      const { databaseUrl, sysDbName } = getDatbaseConfig(configFile);
      const client = await DBOSClient.create(databaseUrl, sysDbName);
      try {
        const output = await client.listWorkflows({
          workflowName: options.name,
          limit: Number(options.limit),
          authenticatedUser: options.user,
          startTime: options.startTime,
          endTime: options.endTime,
          status: options.status as (typeof StatusString)[keyof typeof StatusString],
          applicationVersion: options.applicationVersion,
        });
        console.log(JSON.stringify(output));
      } finally {
        await client.destroy();
      }
    },
  );

workflowCommands
  .command('get')
  .description('Retrieve the status of a workflow')
  .argument('<uuid>', 'Target workflow ID')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(async (uuid: string, options: { appDir?: string }) => {
    const configFile = await readConfigFile(options.appDir);
    const { databaseUrl, sysDbName } = getDatbaseConfig(configFile);
    const client = await DBOSClient.create(databaseUrl, sysDbName);
    try {
      const output = await client.getWorkflow(uuid);
      console.log(JSON.stringify(output));
    } finally {
      await client.destroy();
    }
  });

workflowCommands
  .command('steps')
  .description('List the steps of a workflow')
  .argument('<uuid>', 'Target workflow ID')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(async (uuid: string, options: { appDir?: string }) => {
    const configFile = await readConfigFile(options.appDir);
    const { databaseUrl, sysDbName } = getDatbaseConfig(configFile);
    const client = await DBOSClient.create(databaseUrl, sysDbName);
    try {
      const output = await client.listWorkflowSteps(uuid);
      console.log(JSON.stringify(output));
    } finally {
      await client.destroy();
    }
  });

workflowCommands
  .command('cancel')
  .description('Cancel a workflow so it is no longer automatically retried or restarted')
  .argument('<uuid>', 'Target workflow ID')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(async (uuid: string, options: { appDir?: string }) => {
    const configFile = await readConfigFile(options.appDir);
    const { databaseUrl, sysDbName } = getDatbaseConfig(configFile);
    const client = await DBOSClient.create(databaseUrl, sysDbName);
    try {
      await client.cancelWorkflow(uuid);
    } finally {
      await client.destroy();
    }
  });

workflowCommands
  .command('resume')
  .description('Resume a workflow from the last step it executed, keeping its workflow ID')
  .argument('<uuid>', 'Target workflow ID')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(async (uuid: string, options: { appDir?: string }) => {
    const configFile = await readConfigFile(options.appDir);
    const { databaseUrl, sysDbName } = getDatbaseConfig(configFile);
    const client = await DBOSClient.create(databaseUrl, sysDbName);
    try {
      await client.resumeWorkflow(uuid);
    } finally {
      await client.destroy();
    }
  });

workflowCommands
  .command('restart')
  .description('Restart a workflow from the beginning with a new workflow ID')
  .argument('<uuid>', 'Target workflow ID')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(async (uuid: string, options: { appDir?: string }) => {
    const configFile = await readConfigFile(options.appDir);
    const { databaseUrl, sysDbName } = getDatbaseConfig(configFile);
    const client = await DBOSClient.create(databaseUrl, sysDbName);
    try {
      await client.forkWorkflow(uuid, 0);
    } finally {
      await client.destroy();
    }
  });

const queueCommands = workflowCommands.command('queue').alias('queues').alias('q').description('Manage DBOS queues');
queueCommands
  .command('list')
  .description('List enqueued functions from your application')
  .option('-n, --name <string>', 'Retrieve functions with this name')
  .option('-s, --start-time <string>', 'Retrieve functions starting after this timestamp (ISO 8601 format)')
  .option('-e, --end-time <string>', 'Retrieve functions starting before this timestamp (ISO 8601 format)')
  .option(
    '-S, --status <string>',
    'Retrieve functions with this status (PENDING, SUCCESS, ERROR, RETRIES_EXCEEDED, ENQUEUED, or CANCELLED)',
  )
  .option('-l, --limit <number>', 'Limit the results returned')
  .option('-q, --queue <string>', 'Retrieve functions run on this queue')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(
    async (options: {
      name?: string;
      startTime?: string;
      endTime?: string;
      status?: string;
      limit?: string;
      queue?: string;
      appDir?: string;
    }) => {
      if (
        options.status &&
        !Object.values(StatusString).includes(options.status as (typeof StatusString)[keyof typeof StatusString])
      ) {
        console.error('Invalid status: ', options.status);
        exit(1);
      }

      const configFile = await readConfigFile(options.appDir);
      const { databaseUrl, sysDbName } = getDatbaseConfig(configFile);
      const client = await DBOSClient.create(databaseUrl, sysDbName);
      try {
        // TOD: Review!
        const output = await client.listQueuedWorkflows({
          limit: Number(options.limit),
          startTime: options.startTime,
          endTime: options.endTime,
          status: options.status as (typeof StatusString)[keyof typeof StatusString],
          workflowName: options.name,
          queueName: options.queue,
        });
        console.log(JSON.stringify(output));
      } finally {
        await client.destroy();
      }
    },
  );

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
export async function runAndLog(
  action: (config: DBOSConfigInternal, configFile: ConfigFile, logger: GlobalLogger) => Promise<number> | number,
) {
  let logger = new GlobalLogger();
  const [config] = parseConfigFile();
  const configFile = loadConfigFile(dbosConfigFilePath); // pass the raw config file for CLI arguments
  let terminate = undefined;
  if (configFile.telemetry?.OTLPExporter) {
    logger = new GlobalLogger(
      new TelemetryCollector(new TelemetryExporter(configFile.telemetry.OTLPExporter)),
      configFile.telemetry?.logs,
    );
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
    returnCode = await action(config, configFile, logger);
  } catch (e) {
    logger.error(e);
  }
  terminate(returnCode);
}

function getGlobalLogger(configFile: DBOSConfigInternal): GlobalLogger {
  if (configFile.telemetry?.OTLPExporter) {
    return new GlobalLogger(
      new TelemetryCollector(new TelemetryExporter(configFile.telemetry.OTLPExporter)),
      configFile.telemetry?.logs,
    );
  }
  return new GlobalLogger();
}
