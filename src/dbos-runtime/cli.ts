#!/usr/bin/env node
import {
  dbosConfigFilePath,
  getApplicationDatabaseUrl,
  getDbosConfig,
  getRuntimeConfig,
  getSystemDatabaseUrl,
  overwriteConfigForDBOSCloud,
  readConfigFile,
} from './config';
import { Command } from 'commander';
import { DBOSConfigInternal } from '../dbos-executor';
import { migrate, grantDbosSchemaPermissions } from './migrate';
import { GlobalLogger } from '../telemetry/logs';
import { TelemetryCollector } from '../telemetry/collector';
import { confirm } from '@inquirer/prompts';
import { TelemetryExporter } from '../telemetry/exporters';
import { DBOSClient, GetWorkflowsInput, StatusString } from '..';
import { migrateSystemDatabase, PostgresSystemDatabase } from '../system_database';
import { createDBIfDoesNotExist } from '../user_database';
import { getClientConfig } from '../utils';
import { PoolConfig } from 'pg';
import { exit } from 'node:process';
import { runCommand } from './commands';
import { GetQueuedWorkflowsInput } from '../workflow';
import { startDockerPg, stopDockerPg } from './docker_pg_helper';
import { readFileSync } from '../utils';
import { existsSync } from 'node:fs';

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
  .action(async (options: { loglevel?: string }) => {
    const config = readConfigFile();
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
          const ret = await runCommand(command, logger);
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
  .action(async () => {
    const configFile = readConfigFile();
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
  .option('-r, --app-role <string>', 'The role with which you will run your DBOS application')
  .action(async (systemDatabaseUrl: string | undefined, options: { appRole?: string }) => {
    const logger = new GlobalLogger();

    // Determine system database URL from argument or config
    const databaseURLs = getDatabaseURLs(systemDatabaseUrl);
    systemDatabaseUrl = databaseURLs.systemDatabaseURL;

    try {
      const url = new URL(systemDatabaseUrl);
      const systemDbName = url.pathname.slice(1);

      if (!systemDbName) {
        logger.error('Provided database URL does not specify the system database name');
        process.exit(1);
      }

      await createDBIfDoesNotExist(systemDatabaseUrl, logger);

      const systemPoolConfig: PoolConfig = getClientConfig(systemDatabaseUrl);

      // Load the DBOS system schema.
      logger.info('Creating DBOS system schema');
      await migrateSystemDatabase(systemPoolConfig, logger);

      // Grant permissions to application role if specified
      if (options.appRole) {
        await grantDbosSchemaPermissions(systemDatabaseUrl, options.appRole, logger);
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
  .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
  .action(async (options: { yes: boolean; sysDbUrl?: string }) => {
    if (options.yes) {
      const userConfirmed = await confirm({
        message:
          'This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?',
        default: false, // Default value for confirmation
      });

      if (!userConfirmed) {
        console.log('Operation cancelled.');
        process.exit(0); // Exit the process if the user cancels
      }
    }
    const urls = getDatabaseURLs(options.sysDbUrl);
    await PostgresSystemDatabase.dropSystemDB(urls.systemDatabaseURL);
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
  .option('-t, --start-time <string>', 'Retrieve workflows starting after this timestamp (ISO 8601 format)')
  .option('-e, --end-time <string>', 'Retrieve workflows starting before this timestamp (ISO 8601 format)')
  .option(
    '-S, --status <string>',
    'Retrieve workflows with this status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)',
  )
  .option('-v, --application-version <string>', 'Retrieve workflows with this application version')
  .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
  .action(
    async (options: {
      name?: string;
      limit?: string;
      user?: string;
      startTime?: string;
      endTime?: string;
      status?: string;
      applicationVersion?: string;
      sysDbUrl?: string;
    }) => {
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
      const urls = getDatabaseURLs(options.sysDbUrl);
      const client = DBOSClient.create({
        databaseUrl: urls.applicationDatabaseURL,
        systemDatabaseUrl: urls.systemDatabaseURL,
      });
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
  .argument('<workflowID>', 'Target workflow ID')
  .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
  .action(async (workflowID: string, options: { sysDbUrl?: string }) => {
    const urls = getDatabaseURLs(options.sysDbUrl);
    const client = DBOSClient.create({
      databaseUrl: urls.applicationDatabaseURL,
      systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
      const output = await client.getWorkflow(workflowID);
      console.log(JSON.stringify(output));
    } finally {
      await client.destroy();
    }
  });

workflowCommands
  .command('steps')
  .description('List the steps of a workflow')
  .argument('<workflowID>', 'Target workflow ID')
  .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
  .action(async (workflowID: string, options: { sysDbUrl?: string }) => {
    const urls = getDatabaseURLs(options.sysDbUrl);
    const client = DBOSClient.create({
      databaseUrl: urls.applicationDatabaseURL,
      systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
      const output = await client.listWorkflowSteps(workflowID);
      console.log(JSON.stringify(output));
    } finally {
      await client.destroy();
    }
  });

workflowCommands
  .command('cancel')
  .description('Cancel a workflow so it is no longer automatically retried or restarted')
  .argument('<workflowID>', 'Target workflow ID')
  .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
  .action(async (workflowID: string, options: { sysDbUrl?: string }) => {
    const urls = getDatabaseURLs(options.sysDbUrl);
    const client = DBOSClient.create({
      databaseUrl: urls.applicationDatabaseURL,
      systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
      await client.cancelWorkflow(workflowID);
    } finally {
      await client.destroy();
    }
  });

workflowCommands
  .command('resume')
  .description('Resume a workflow from the last step it executed, keeping its workflow ID')
  .argument('<workflowID>', 'Target workflow ID')
  .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
  .action(async (workflowID: string, options: { sysDbUrl?: string }) => {
    const urls = getDatabaseURLs(options.sysDbUrl);
    const client = DBOSClient.create({
      databaseUrl: urls.applicationDatabaseURL,
      systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
      await client.resumeWorkflow(workflowID);
    } finally {
      await client.destroy();
    }
  });

workflowCommands
  .command('restart')
  .description('Restart a workflow from the beginning with a new workflow ID')
  .argument('<workflowID>', 'Target workflow ID')
  .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
  .action(async (workflowID: string, options: { sysDbUrl?: string }) => {
    const urls = getDatabaseURLs(options.sysDbUrl);
    const client = DBOSClient.create({
      databaseUrl: urls.applicationDatabaseURL,
      systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
      await client.forkWorkflow(workflowID, 0);
    } finally {
      await client.destroy();
    }
  });

const queueCommands = workflowCommands.command('queue').alias('queues').alias('q').description('Manage DBOS queues');
queueCommands
  .command('list')
  .description('List enqueued functions from your application')
  .option('-n, --name <string>', 'Retrieve functions with this name')
  .option('-t, --start-time <string>', 'Retrieve functions starting after this timestamp (ISO 8601 format)')
  .option('-e, --end-time <string>', 'Retrieve functions starting before this timestamp (ISO 8601 format)')
  .option(
    '-S, --status <string>',
    'Retrieve functions with this status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)',
  )
  .option('-l, --limit <number>', 'Limit the results returned')
  .option('-q, --queue <string>', 'Retrieve functions run on this queue')
  .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
  .action(
    async (options: {
      name?: string;
      startTime?: string;
      endTime?: string;
      status?: string;
      limit?: string;
      queue?: string;
      sysDbUrl?: string;
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
      const urls = getDatabaseURLs(options.sysDbUrl);
      const client = DBOSClient.create({
        databaseUrl: urls.applicationDatabaseURL,
        systemDatabaseUrl: urls.systemDatabaseURL,
      });
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

function getDatabaseURLs(systemDatabaseURL: string | undefined): {
  applicationDatabaseURL?: string;
  systemDatabaseURL: string;
} {
  if (process.env.DBOS__CLOUD === 'true') {
    return {
      applicationDatabaseURL: process.env.DBOS_DATABASE_URL!,
      systemDatabaseURL: process.env.DBOS_SYSTEM_DATABASE_URL!,
    };
  }
  if (systemDatabaseURL) {
    return { applicationDatabaseURL: undefined, systemDatabaseURL: systemDatabaseURL };
  }
  if (existsSync(dbosConfigFilePath)) {
    const config = readConfigFile();
    return {
      applicationDatabaseURL: config.database_url,
      systemDatabaseURL: getSystemDatabaseUrl(config),
    };
  } else {
    throw new Error('Error: Missing database URL: please set it using CLI flags or your dbos-config.yaml file.');
  }
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
