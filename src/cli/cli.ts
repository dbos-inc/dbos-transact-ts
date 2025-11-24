#!/usr/bin/env node
import {
  dbosConfigFilePath,
  getDbosConfig,
  getRuntimeConfig,
  getSystemDatabaseUrl,
  overwriteConfigForDBOSCloud,
  readConfigFile,
} from '../config';
import { Command } from 'commander';
import { DBOSConfigInternal } from '../dbos-executor';
import { migrate } from './migrate';
import { GlobalLogger } from '../telemetry/logs';
import { TelemetryCollector } from '../telemetry/collector';
import { TelemetryExporter } from '../telemetry/exporters';
import * as readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { DBOSClient, GetWorkflowsInput, StatusString } from '..';
import { ensureSystemDatabase, grantDbosSchemaPermissions } from '../system_database';
import { exit } from 'node:process';
import { runCommand } from './commands';
import { startDockerPg, stopDockerPg } from './docker_pg_helper';
import { dropPGDatabase, getDatabaseNameFromUrl } from '../database_utils';
import { existsSync } from 'node:fs';
import { globalParams } from '../utils';

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

program.version(globalParams.dbosVersion);

program
  .command('start')
  .description('Start the server')
  .option('-l, --loglevel <string>', 'Specify log level')
  .action(async (options: { loglevel?: string }) => {
    const config = await readConfigFile();
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
    const configFile = await readConfigFile();
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
  .option('-s, --schema <string>', 'The schema name for DBOS system tables (default: dbos)')
  .action(async (systemDatabaseUrl: string | undefined, options: { appRole?: string; schema?: string }) => {
    const logger = new GlobalLogger();

    // Determine system database URL from argument or config
    const databaseURLs = await getDatabaseURLs(systemDatabaseUrl);
    systemDatabaseUrl = databaseURLs.systemDatabaseURL;

    // Get schema name from CLI option first, then config, then default
    let schemaName = options.schema ?? 'dbos';
    if (!options.schema) {
      try {
        if (existsSync(dbosConfigFilePath)) {
          const configFile = await readConfigFile(dbosConfigFilePath);
          schemaName = configFile.system_database_schema_name ?? 'dbos';
        }
      } catch (e) {
        // If config file doesn't exist or can't be read, use default
      }
    }

    try {
      // Load the DBOS system schema.
      logger.info(`Creating DBOS system database and schema: ${schemaName}`);
      await ensureSystemDatabase(systemDatabaseUrl, logger, false, undefined, schemaName);

      // Grant permissions to application role if specified
      if (options.appRole) {
        await grantDbosSchemaPermissions(systemDatabaseUrl, options.appRole, logger, schemaName);
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
    if (!options.yes) {
      const rl = readline.createInterface({ input, output });
      try {
        const answer = await rl.question(
          'This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed? (y/N) ',
        );
        const userConfirmed = answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes';

        if (!userConfirmed) {
          console.log('Operation cancelled.');
          process.exit(0); // Exit the process if the user cancels
        }
      } finally {
        rl.close();
      }
    }
    const urls = await getDatabaseURLs(options.sysDbUrl);

    const res = await dropPGDatabase({
      urlToDrop: urls.systemDatabaseURL,
      logger: (msg: string) => console.log(msg),
    });

    const sysDbName = getDatabaseNameFromUrl(urls.systemDatabaseURL);

    if (res.status === 'dropped') {
      console.log(`Dropped '${sysDbName}'.  To use DBOS in the future, you will need to create a new system database.`);
    } else if (res.status === 'did_not_exist') {
      console.log(
        `Database '${sysDbName} was already dropped'.  To use DBOS in the future, you will need to create a new system database.`,
      );
    } else if (res.status === 'failed') {
      console.log(
        `DROP operation for '${sysDbName} could not be attempted: \n ${res.notes.join('\n')} ${res.hint ?? ''}.`,
      );
    }
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
      const urls = await getDatabaseURLs(options.sysDbUrl);
      const client = await DBOSClient.create({
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
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await DBOSClient.create({
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
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await DBOSClient.create({
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
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await DBOSClient.create({
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
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await DBOSClient.create({
      systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
      await client.resumeWorkflow(workflowID);
    } finally {
      await client.destroy();
    }
  });

workflowCommands
  .command('fork')
  .description('Fork a workflow from a step with a new ID')
  .argument('<workflowID>', 'Target workflow ID')
  .requiredOption('-S, --step <number>', 'Restart from this step')
  .option('-f, --forked-workflow-id <string>', 'Custom ID for the forked workflow')
  .option('-v, --application-version <string>', 'Custom application version for the forked workflow')
  .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
  .action(
    async (
      workflowID: string,
      options: { step: number; forkedWorkflowId?: string; applicationVersion?: string; sysDbUrl?: string },
    ) => {
      const urls = await getDatabaseURLs(options.sysDbUrl);
      const client = await DBOSClient.create({
        systemDatabaseUrl: urls.systemDatabaseURL,
      });
      try {
        await client.forkWorkflow(workflowID, options.step, {
          newWorkflowID: options.forkedWorkflowId,
          applicationVersion: options.applicationVersion,
        });
      } finally {
        await client.destroy();
      }
    },
  );

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

      const input: GetWorkflowsInput = {
        limit: Number(options.limit),
        startTime: options.startTime,
        endTime: options.endTime,
        status: options.status as GetWorkflowsInput['status'],
        workflowName: options.name,
        queueName: options.queue,
      };
      const urls = await getDatabaseURLs(options.sysDbUrl);
      const client = await DBOSClient.create({
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

async function getDatabaseURLs(systemDatabaseURL: string | undefined): Promise<{
  systemDatabaseURL: string;
}> {
  if (process.env.DBOS__CLOUD === 'true') {
    return {
      systemDatabaseURL: process.env.DBOS_SYSTEM_DATABASE_URL!,
    };
  }
  if (systemDatabaseURL) {
    return { systemDatabaseURL: systemDatabaseURL };
  }
  if (existsSync(dbosConfigFilePath)) {
    const config = await readConfigFile();
    return {
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
  action: (migrationCommands: string[], systemDatabaseUrl: string, logger: GlobalLogger) => Promise<number> | number,
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
    returnCode = await action(migrationCommands, config.systemDatabaseUrl, logger);
  } catch (e) {
    logger.error(e);
  }
  terminate(returnCode);
}
