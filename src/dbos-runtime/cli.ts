#!/usr/bin/env node
import { DBOSRuntime } from './runtime';
import {
  ConfigFile,
  getDatabaseUrl,
  getDbosConfig,
  getRuntimeConfig,
  getSystemDatabaseUrl,
  readConfigFile,
} from './config';
import { Command } from 'commander';
import { DBOSConfigInternal } from '../dbos-executor';
import { debugWorkflow } from './debug';
import { migrate } from './migrate';
import { GlobalLogger } from '../telemetry/logs';
import { TelemetryCollector } from '../telemetry/collector';
import { TelemetryExporter } from '../telemetry/exporters';
import { DBOSClient, GetWorkflowsInput, StatusString } from '..';
import { exit } from 'node:process';
import { runCommand } from './commands';
import { reset } from './reset';
import { GetQueuedWorkflowsInput } from '../workflow';
import { startDockerPg, stopDockerPg } from './docker_pg_helper';
import { readFileSync } from '../utils';

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

program.version(getDbosVersion());

function getDbosVersion(): string {
  try {
    const contents = readFileSync('../../../package.json');
    const pkg = JSON.parse(contents) as { version: string };
    return pkg.version;
  } catch {
    return 'unknown';
  }
}

program
  .command('start')
  .description('Start the server')
  .option('-p, --port <number>', 'Specify the port number')
  .option('-l, --loglevel <string>', 'Specify log level')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .action(async (options: { port?: number; loglevel?: string; appDir?: string; appVersion?: string | boolean }) => {
    const config = readConfigFile(options.appDir);
    const dbosConfig = getDbosConfig(config, { logLevel: options.loglevel, appVersion: options.appVersion });
    const runtimeConfig = getRuntimeConfig(config, options);

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

    function getGlobalLogger(configFile: DBOSConfigInternal): GlobalLogger {
      if (configFile.telemetry?.OTLPExporter) {
        return new GlobalLogger(
          new TelemetryCollector(new TelemetryExporter(configFile.telemetry.OTLPExporter)),
          configFile.telemetry?.logs,
        );
      }
      return new GlobalLogger();
    }
  });

program
  .command('debug')
  .description('Debug a workflow')
  .requiredOption('-u, --uuid <string>', 'Specify the workflow UUID to replay')
  .option('-l, --loglevel <string>', 'Specify log level')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .action(
    async (options: {
      uuid: string; // Workflow UUID
      loglevel?: string;
      appDir?: string;
      appVersion?: string | boolean;
    }) => {
      const config = readConfigFile(options.appDir);
      const dbosConfig = getDbosConfig(config, {
        logLevel: options.loglevel,
        appVersion: options.appVersion,
        forceConsole: true,
      });
      const runtimeConfig = getRuntimeConfig(config);
      await debugWorkflow(dbosConfig, runtimeConfig, options.uuid);
    },
  );
program
  .command('migrate')
  .description('Perform a database migration')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(async (options: { appDir?: string }) => {
    const config = readConfigFile(options.appDir);
    await runAndLog(config, migrate);
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
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(async (options: { yes: boolean; appDir?: string }) => {
    const logger = new GlobalLogger();
    const config = readConfigFile(options.appDir);
    await reset(config, logger, options.yes);
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
    'Retrieve workflows with this status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)',
  )
  .option('-v, --application-version <string>', 'Retrieve workflows with this application version')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(
    async (options: {
      name?: string;
      limit?: string;
      user?: string;
      startTime?: string;
      endTime?: string;
      status?: string;
      applicationVersion?: string;
      appDir?: string;
    }) => {
      const config = readConfigFile(options.appDir);
      const databaseUrl = getDatabaseUrl(config);
      const validStatuses = Object.values(StatusString) as readonly string[];

      if (options.status && !validStatuses.includes(options.status)) {
        console.error('Invalid status: ', options.status);
        exit(1);
      }

      const input: GetWorkflowsInput = {
        workflowName: options.name,
        limit: Number(options.limit),
        authenticatedUser: options.user,
        startTime: options.startTime,
        endTime: options.endTime,
        status: options.status as GetWorkflowsInput['status'],
        applicationVersion: options.applicationVersion,
      };
      const sysDbUrl = getSystemDatabaseUrl(config);
      const client = await DBOSClient.create(databaseUrl, sysDbUrl);
      try {
        const output = await client.listWorkflows(input);
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
    const config = readConfigFile(options.appDir);
    const databaseUrl = getDatabaseUrl(config);
    const sysDbUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create(databaseUrl, sysDbUrl);
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
  .action(async (uuid: string, options: { appDir?: string; request: boolean; silent: boolean }) => {
    const config = readConfigFile(options.appDir);
    const databaseUrl = getDatabaseUrl(config);
    const sysDbUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create(databaseUrl, sysDbUrl);
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
    const config = readConfigFile(options.appDir);
    const databaseUrl = getDatabaseUrl(config);
    const sysDbUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create(databaseUrl, sysDbUrl);
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
  .action(async (uuid: string, options: { appDir?: string; host: string }) => {
    const config = readConfigFile(options.appDir);
    const databaseUrl = getDatabaseUrl(config);
    const sysDbUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create(databaseUrl, sysDbUrl);
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
  .action(async (uuid: string, options: { appDir?: string; host: string }) => {
    const config = readConfigFile(options.appDir);
    const databaseUrl = getDatabaseUrl(config);
    const sysDbUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create(databaseUrl, sysDbUrl);
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
    'Retrieve functions with this status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)',
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
      const validStatuses = Object.values(StatusString) as readonly string[];
      if (options.status && !validStatuses.includes(options.status)) {
        console.error('Invalid status: ', options.status);
        exit(1);
      }

      const input: GetQueuedWorkflowsInput = {
        limit: Number(options.limit),
        startTime: options.startTime,
        endTime: options.endTime,
        status: options.status as GetQueuedWorkflowsInput['status'],
        workflowName: options.name,
        queueName: options.queue,
      };
      const config = readConfigFile(options.appDir);
      const databaseUrl = getDatabaseUrl(config);
      const sysDbUrl = getSystemDatabaseUrl(config);
      const client = await DBOSClient.create(databaseUrl, sysDbUrl);
      try {
        // TOD: Review!
        const output = await client.listQueuedWorkflows(input);
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
  configFile: ConfigFile,
  action: (configFile: ConfigFile, logger: GlobalLogger) => Promise<number> | number,
) {
  let logger = new GlobalLogger();
  let terminate = undefined;
  if (configFile.telemetry?.OTLPExporter) {
    logger = new GlobalLogger(
      new TelemetryCollector(
        new TelemetryExporter({
          logsEndpoint: toArray(configFile.telemetry?.OTLPExporter?.logsEndpoint),
          tracesEndpoint: toArray(configFile.telemetry?.OTLPExporter?.tracesEndpoint),
        }),
      ),
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
    returnCode = await action(configFile, logger);
  } catch (e) {
    logger.error(e);
  }
  terminate(returnCode);

  function toArray(endpoint: string | string[] | undefined): Array<string> {
    return endpoint ? (Array.isArray(endpoint) ? endpoint : [endpoint]) : [];
  }
}
