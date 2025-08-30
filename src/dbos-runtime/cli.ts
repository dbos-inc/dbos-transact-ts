#!/usr/bin/env node
import {
  getDatabaseUrl,
  getDbosConfig,
  getRuntimeConfig,
  getSystemDatabaseUrl,
  overwriteConfigForDBOSCloud,
  readConfigFile,
} from './config';
import { Command } from 'commander';
import { DBOSConfigInternal } from '../dbos-executor';
import { migrate } from './migrate';
import { GlobalLogger } from '../telemetry/logs';
import { TelemetryCollector } from '../telemetry/collector';
import { TelemetryExporter } from '../telemetry/exporters';
import { DBOSClient, GetWorkflowsInput, StatusString } from '..';
import { ensureSystemDatabase, grantDbosSchemaPermissions } from '../system_database';
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
  .option('-l, --loglevel <string>', 'Specify log level')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(async (options: { loglevel?: string; appDir?: string }) => {
    const config = readConfigFile(options.appDir);
    const dbosConfig = getDbosConfig(config, { logLevel: options.loglevel });
    const runtimeConfig = getRuntimeConfig(config);

    // If no start commands are provided, start the DBOS runtime
    if (runtimeConfig.start.length === 0) {
      console.error('No start commands provided in the configuration file.');
      exit(1);
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
  .command('migrate')
  .description('Perform a database migration')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .action(async (options: { appDir?: string }) => {
    const configFile = readConfigFile(options.appDir);
    let config = getDbosConfig(configFile);
    const runtimeConfig = getRuntimeConfig(configFile);
    if (process.env.DBOS__CLOUD === 'true') {
      [config] = overwriteConfigForDBOSCloud(config, runtimeConfig, configFile);
    }

    await runAndLog(configFile.database?.migrate ?? [], config, migrate);
  });

program
  .command('schema')
  .description('Create the DBOS system database and its internal tables')
  .argument('[systemDatabaseUrl]', 'System database URL')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .option('-r, --app-role <string>', 'The role with which you will run your DBOS application')
  .action(async (systemDatabaseUrl: string | undefined, options: { appDir?: string; appRole?: string }) => {
    const logger = new GlobalLogger();

    // Determine system database URL from argument or config
    let finalSystemDatabaseUrl = systemDatabaseUrl;
    if (!finalSystemDatabaseUrl) {
      try {
        const configFile = readConfigFile(options.appDir);
        finalSystemDatabaseUrl = getSystemDatabaseUrl(configFile);
      } catch {
        // Config doesn't have system database URL
      }
    }

    if (!finalSystemDatabaseUrl) {
      logger.error('System database URL must be provided as argument or in dbos-config.yaml');
      process.exit(1);
    }

    try {
      // Load the DBOS system schema.
      logger.info('Creating DBOS system database and schema');
      await ensureSystemDatabase(finalSystemDatabaseUrl, logger);

      // Grant permissions to application role if specified
      if (options.appRole) {
        await grantDbosSchemaPermissions(finalSystemDatabaseUrl, options.appRole, logger);
      }
    } catch (e) {
      logger.error(e);
      process.exit(1);
    }
  });

program
  .command('postgres')
  .alias('pg')
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
      const systemDatabaseUrl = getSystemDatabaseUrl(config);
      const client = await DBOSClient.create({ databaseUrl, systemDatabaseUrl });
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
    const systemDatabaseUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create({ databaseUrl, systemDatabaseUrl });
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
    const systemDatabaseUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create({ databaseUrl, systemDatabaseUrl });
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
    const systemDatabaseUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create({ databaseUrl, systemDatabaseUrl });
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
    const systemDatabaseUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create({ databaseUrl, systemDatabaseUrl });
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
    const systemDatabaseUrl = getSystemDatabaseUrl(config);
    const client = await DBOSClient.create({ databaseUrl, systemDatabaseUrl });
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
      const systemDatabaseUrl = getSystemDatabaseUrl(config);
      const client = await DBOSClient.create({ databaseUrl, systemDatabaseUrl });
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
  migrationCommands: string[],
  config: DBOSConfigInternal,
  action: (
    migrationCommands: string[],
    databaseUrl: string,
    systemDatabaseUrl: string,
    logger: GlobalLogger,
  ) => Promise<number> | number,
) {
  let logger = new GlobalLogger();
  let terminate = undefined;
  if (config.telemetry.OTLPExporter) {
    logger = new GlobalLogger(
      new TelemetryCollector(
        new TelemetryExporter({
          logsEndpoint: config.telemetry.OTLPExporter.logsEndpoint ?? [],
          tracesEndpoint: config.telemetry.OTLPExporter.tracesEndpoint ?? [],
        }),
      ),
      config.telemetry?.logs,
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
    returnCode = await action(migrationCommands, config.databaseUrl, config.systemDatabaseUrl, logger);
  } catch (e) {
    logger.error(e);
  }
  terminate(returnCode);
}
