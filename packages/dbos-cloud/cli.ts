#!/usr/bin/env node

import {
  registerApp,
  updateApp,
  listApps,
  deleteApp,
  deployAppCode,
  getAppLogs,
  createSecret,
  listSecrets,
  deleteSecret,
} from './applications/index.js';
import { Command } from 'commander';
import { login } from './users/login.js';
import { registerUser } from './users/register.js';
import {
  createUserDb,
  getUserDb,
  deleteUserDb,
  listUserDB,
  resetDBCredentials,
  linkUserDB,
  unlinkUserDB,
  restoreUserDB,
  connect,
} from './databases/databases.js';
import { launchDashboard, getDashboardURL, deleteDashboard } from './dashboards/dashboards.js';
import { DBOSCloudHost, credentialsExist, defaultConfigFilePath, deleteCredentials, getLogger } from './cloudutils.js';
import { getAppInfo } from './applications/get-app-info.js';
import promptSync from 'prompt-sync';
import chalk from 'chalk';
import fs from 'fs';
import { fileURLToPath } from 'url';
import path from 'path';
import updateNotifier, { Package } from 'update-notifier';
import { profile } from './users/profile.js';
import { revokeRefreshToken } from './users/authentication.js';
import { listAppVersions } from './applications/list-app-versions.js';
import {
  orgInvite,
  orgListUsers,
  renameOrganization,
  joinOrganization,
  removeUserFromOrg,
} from './organizations/organization.js';
import {
  ListQueuedWorkflowsInput,
  ListWorkflowsInput,
  cancelWorkflow,
  listQueuedWorkflows,
  listWorkflows,
  restartWorkflow,
  resumeWorkflow,
} from './applications/manage-workflows.js';
import { importSecrets } from './applications/secrets.js';

// Read local package.json
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const packageJson = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json')).toString()) as Package;

// Notify the user if the package requires an update.
try {
  const notifier = updateNotifier({
    pkg: packageJson,
    updateCheckInterval: 0,
  });
  if (
    notifier.update &&
    !notifier.update.current.includes('preview') &&
    !notifier.update.current.includes('placeholder') &&
    notifier.update.current !== notifier.update.latest
  ) {
    console.log(`
  ${chalk.yellow('-----------------------------------------------------------------------------------------')}

  DBOS Cloud CLI Update available ${chalk.gray(notifier.update.current)} →  ${chalk.green(notifier.update.latest)}

  To upgrade the DBOS Cloud CLI to the latest version, run the following command:
  ${chalk.cyan('`npm i -g @dbos-inc/dbos-cloud@latest`')}

  ${chalk.yellow('-----------------------------------------------------------------------------------------')}`);
  }
} catch (error) {
  // Ignore errors in the notifier
}

const program = new Command();

program.version(packageJson.version);

/////////////////////
/* AUTHENTICATION  */
/////////////////////

program
  .command('login')
  .description('Log in to DBOS cloud')
  .option('--get-refresh-token', 'Get a refresh token you can use to programatically log in')
  .option('--with-refresh-token <token>', 'Use a refresh token to programatically log in')
  .action(async (options: { getRefreshToken?: boolean; withRefreshToken?: string }) => {
    const exitCode = await login(DBOSCloudHost, options.getRefreshToken || false, options.withRefreshToken);
    process.exit(exitCode);
  });

program
  .command('profile')
  .description('Get user information')
  .option('--json', 'Emit JSON output')
  .action(async (options: { json: boolean }) => {
    const exitCode = await profile(DBOSCloudHost, options.json);
    process.exit(exitCode);
  });

program
  .command('register')
  .description('Register a user with DBOS cloud')
  .requiredOption('-u, --username <string>', 'Username')
  .option('-s, --secret <string>', 'Organization secret')
  .action(async (options: { username: string; secret: string | undefined }) => {
    const exitCode = await registerUser(options.username, options.secret, DBOSCloudHost);
    process.exit(exitCode);
  });

program
  .command('logout')
  .description('Log out of DBOS cloud')
  .action(() => {
    if (credentialsExist()) {
      deleteCredentials();
    }
    process.exit(0);
  });

program
  .command('revoke')
  .description('Revoke a refresh token')
  .argument('<token>', 'Token to revoke')
  .action(async (token: string) => {
    const exitCode = await revokeRefreshToken(getLogger(), token);
    process.exit(exitCode);
  });

/////////////////////////////
/* APPLICATIONS MANAGEMENT */
/////////////////////////////

const applicationCommands = program
  .command('application')
  .alias('applications')
  .alias('app')
  .alias('apps')
  .description('Manage your DBOS applications');

applicationCommands
  .command('register')
  .description('Register this application')
  .argument('[string]', 'application name (Default: name from package.json)')
  .requiredOption('-d, --database <string>', 'Specify a Postgres database instance for this application')
  .option('--enable-timetravel', 'Enable time travel for the application', false)
  .option('--executors-memory-mib <number>', 'Specify the memory in MiB for the executors of this application')
  .action(
    async (
      appName: string | undefined,
      options: { database: string; enableTimetravel: boolean; executorsMemoryMib?: number },
    ) => {
      const exitCode = await registerApp(
        options.database,
        DBOSCloudHost,
        options.enableTimetravel,
        appName,
        options.executorsMemoryMib,
      );
      process.exit(exitCode);
    },
  );

applicationCommands
  .command('update')
  .description('Update this application')
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--executors-memory-mib <number>', 'Specify the memory in MiB for the executors of this application')
  .option('--min-executors <number>', 'Specify the minimum number of executors the app should scale to')
  .action(async (appName: string | undefined, options: { executorsMemoryMib?: number; minExecutors?: number }) => {
    const exitCode = await updateApp(DBOSCloudHost, appName, options.executorsMemoryMib, options.minExecutors);
    process.exit(exitCode);
  });

applicationCommands
  .command('deploy')
  .description('Deploy this application to the cloud and run associated database migration commands')
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('-p, --previous-version <string>', 'Specify a previous version to restore')
  .option(
    '-d, --database <string>',
    'Specify a Postgres database instance for this application. This cannot be changed after the application is first deployed.',
  )
  .option(
    '--enable-timetravel',
    'Enable time travel for the application. This cannot be changed after the application is first deployed.',
    false,
  )
  .option('--verbose', 'Verbose log of deployment step')
  .option('--configFile <string>', 'DBOS Config file path', defaultConfigFilePath)
  .action(
    async (
      appName: string | undefined,
      options: {
        verbose?: boolean;
        previousVersion?: string;
        database?: string;
        enableTimetravel: boolean;
        configFile: string;
      },
    ) => {
      const exitCode = await deployAppCode(
        DBOSCloudHost,
        false,
        options.previousVersion ?? null,
        options.verbose ?? false,
        null,
        appName,
        options.configFile,
        options.database,
        options.enableTimetravel,
      );
      process.exit(exitCode);
    },
  );

applicationCommands
  .command('change-database-instance')
  .description("Change this application's database instance and redeploy it")
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--verbose', 'Verbose log of deployment step')
  .option('-p, --previous-version <string>', 'Specify a previous version to restore')
  .requiredOption('-d, --database <string>', 'Specify the new database instance name for this application')
  .option('--configFile', 'DBOS Config file path', defaultConfigFilePath)
  .action(
    async (
      appName: string | undefined,
      options: { verbose?: boolean; previousVersion?: string; database: string; configFile: string },
    ) => {
      const exitCode = await deployAppCode(
        DBOSCloudHost,
        false,
        options.previousVersion ?? null,
        options.verbose ?? false,
        options.database,
        appName,
        options.configFile,
      );
      process.exit(exitCode);
    },
  );

applicationCommands
  .command('delete')
  .description('Delete this application')
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--dropdb', 'Drop application database')
  .action(async (appName: string | undefined, options: { dropdb: boolean }) => {
    const exitCode = await deleteApp(DBOSCloudHost, options.dropdb, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('list')
  .description('List all applications')
  .option('--json', 'Emit JSON output')
  .action(async (options: { json: boolean }) => {
    const exitCode = await listApps(DBOSCloudHost, options.json);
    process.exit(exitCode);
  });

applicationCommands
  .command('status')
  .description("Retrieve this application's status")
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--json', 'Emit JSON output')
  .action(async (appName: string | undefined, options: { json: boolean }) => {
    const exitCode = await getAppInfo(DBOSCloudHost, options.json, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('versions')
  .description("Retrieve a list of an application's versions")
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--json', 'Emit JSON output')
  .action(async (appName: string | undefined, options: { json: boolean }) => {
    const exitCode = await listAppVersions(DBOSCloudHost, options.json, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('resource-usage')
  .description("Query resource usage for your applications")
  .option(
    '-l, --last <integer>',
    'How far back to query, in seconds from current time. By default, returns data for the last hour',
    parseInt,
  )
  .option('-g, --group-by <string>', 'Time interval for grouping data: \'minute\', \'hour\', or \'day\'')
  .action(async (options: { last: number; pagesize: number }) => {
    const exitCode = await getAppLogs(DBOSCloudHost, options.last, options.pagesize, appName);
    process.exit(exitCode);
  });

applicationCommands
  .command('logs')
  .description("Print this application's logs")
  .argument('[string]', 'application name (Default: name from package.json)')
  .option(
    '-l, --last <integer>',
    'How far back to query, in seconds from current time. By default, we retrieve all data',
    parseInt,
  )
  .option('-p, --pagesize <integer>', 'How many lines to fetch at once when paginating. Default is 1000', parseInt)
  .action(async (appName: string | undefined, options: { last: number; pagesize: number }) => {
    const exitCode = await getAppLogs(DBOSCloudHost, options.last, options.pagesize, appName);
    process.exit(exitCode);
  });

const secretsCommands = applicationCommands
  .command('env')
  .alias('environment')
  .alias('secrets')
  .alias('sec')
  .alias('secret')
  .description('Manage your application environment variables');

secretsCommands
  .command('create')
  .description('Create an environment variable for this application')
  .argument('[string]', 'application name (Default: name from package.json)')
  .requiredOption('-s, --name <string>', 'Specify the name of the variable to create')
  .requiredOption('-v, --value <string>', 'Specify the value of the variable')
  .action(async (appName: string | undefined, options: { name: string; value: string }) => {
    const exitCode = await createSecret(DBOSCloudHost, appName, options.name, options.value);
    process.exit(exitCode);
  });

secretsCommands
  .command('import')
  .description('Import environment variables from a dotenv file')
  .argument('[string]', 'application name (Default: name from package.json)')
  .requiredOption('-d, --dotenv <string>', 'Path to a dotenv file')
  .action(async (appName: string | undefined, options: { dotenv: string }) => {
    const exitCode = await importSecrets(DBOSCloudHost, appName, options.dotenv);
    process.exit(exitCode);
  });

secretsCommands
  .command('list')
  .description('List environment variables for this application')
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('--json', 'Emit JSON output')
  .action(async (appName: string | undefined, options: { json: boolean }) => {
    const exitCode = await listSecrets(DBOSCloudHost, appName, options.json);
    process.exit(exitCode);
  });

secretsCommands
  .command('delete')
  .description('Delete an environment variable for this application')
  .argument('[string]', 'application name (Default: name from package.json)')
  .requiredOption('-s, --name <string>', 'Specify the name of the variable to delete')
  .action(async (appName: string | undefined, options: { name: string }) => {
    const exitCode = await deleteSecret(DBOSCloudHost, appName, options.name);
    process.exit(exitCode);
  });

//////////////////////////////////
/* DATABASE INSTANCE MANAGEMENT */
//////////////////////////////////

const databaseCommands = program
  .command('database')
  .alias('databases')
  .alias('db')
  .description('Manage Postgres database instances');

const prompt = promptSync({ sigint: true });
databaseCommands
  .command('provision')
  .description('Provision a Postgres database instance')
  .argument('<name>', 'database instance name')
  .requiredOption('-U, --username <string>', 'Specify your database username')
  .option('-W, --password <string>', 'Specify your database user password')
  .action(async (dbname: string, options: { username: string; password: string | undefined }) => {
    if (!options.password) {
      options.password = prompt('Database Password: ', { echo: '*' });
    }
    const exitCode = await createUserDb(DBOSCloudHost, dbname, options.username, options.password, true);
    process.exit(exitCode);
  });

databaseCommands
  .command('status')
  .description('Retrieve the status of a Postgres database instance')
  .argument('<name>', 'database instance name')
  .option('--json', 'Emit JSON output')
  .action(async (dbname: string, options: { json: boolean }) => {
    const exitCode = await getUserDb(DBOSCloudHost, dbname, options.json);
    process.exit(exitCode);
  });

databaseCommands
  .command('list')
  .description('List all your Postgres database instances')
  .option('--json', 'Emit JSON output')
  .action(async (options: { json: boolean }) => {
    const exitCode = await listUserDB(DBOSCloudHost, options.json);
    process.exit(exitCode);
  });

databaseCommands
  .command('reset-password')
  .description('Reset password for a Postgres database instance')
  .argument('[name]', 'database instance name')
  .option('-W, --password <string>', 'Specify the database user password')
  .action(async (dbName: string | undefined, options: { password: string }) => {
    const exitCode = await resetDBCredentials(DBOSCloudHost, dbName, options.password);
    process.exit(exitCode);
  });

databaseCommands
  .command('destroy')
  .description('Destroy a Postgres database instance')
  .argument('<name>', 'database instance name')
  .action(async (dbname: string) => {
    const exitCode = await deleteUserDb(DBOSCloudHost, dbname);
    process.exit(exitCode);
  });

databaseCommands
  .command('restore')
  .description('Restore a Postgres database instance to a specified point in time')
  .argument('<name>', 'database instance name')
  .requiredOption(
    '-t, --restore-time <string>',
    'Specify the point in time to which to restore the database. Must be a timestamp in RFC 3339 format. Example: 2009-09-07T23:45:00Z',
  )
  .requiredOption('-n, --target-name <string>', 'Specify the new database instance name')
  .action(async (dbname: string, options: { restoreTime: string; targetName: string }) => {
    const exitCode = await restoreUserDB(DBOSCloudHost, dbname, options.targetName, options.restoreTime, true);
    process.exit(exitCode);
  });

databaseCommands
  .command('link')
  .description('Link your own Postgres database instance to DBOS Cloud')
  .argument('<name>', 'database instance name')
  .requiredOption('-H, --hostname <string>', 'Specify your database hostname')
  .option('-p, --port <number>', 'Specify your database port', '5432')
  .option('-W, --password <string>', 'Specify password for the dbosadmin user')
  .option('--enable-timetravel', 'Enable time travel on the linked database', false)
  .option('--supabase-ref <string>', 'Link a Supabase database')
  .action(
    async (
      dbname: string,
      options: {
        hostname: string;
        port: string;
        password: string | undefined;
        enableTimetravel: boolean;
        supabaseRef: string | undefined;
      },
    ) => {
      if (!options.password) {
        options.password = prompt('Password for the dbosadmin user: ', { echo: '*' });
      }
      const exitCode = await linkUserDB(
        DBOSCloudHost,
        dbname,
        options.hostname,
        Number(options.port),
        options.password,
        options.enableTimetravel,
        options.supabaseRef,
      );
      process.exit(exitCode);
    },
  );

databaseCommands
  .command('unlink')
  .description('Unlink a Postgres database instance')
  .argument('<name>', 'database instance name')
  .action(async (dbname: string) => {
    const exitCode = await unlinkUserDB(DBOSCloudHost, dbname);
    process.exit(exitCode);
  });

databaseCommands
  .command('url')
  .alias('connect')
  .description(`Display your cloud database connection URL`)
  .argument('[name]', 'database instance name')
  .option('-W, --password <string>', 'Specify the database user password')
  .option('-S, --show-password', 'Show the password in the output')
  .action(async (dbname: string | undefined, options: { password: string | undefined; showPassword: boolean }) => {
    const exitCode = await connect(DBOSCloudHost, dbname, options.password, options.showPassword);
    process.exit(exitCode);
  });

/////////////////////
/* USER DASHBOARDS */
/////////////////////

const dashboardCommands = program.command('dashboard').description('Manage Monitoring Dashboards');

dashboardCommands
  .command('launch')
  .description('Deploy the Monitoring Dashboard')
  .action(async () => {
    const exitCode = await launchDashboard(DBOSCloudHost);
    process.exit(exitCode);
  });

dashboardCommands
  .command('url')
  .description('Deploy the Monitoring Dashboard if it does not exist; then return its URL')
  .action(async () => {
    const exitCode = await getDashboardURL(DBOSCloudHost);
    process.exit(exitCode);
  });

dashboardCommands
  .command('delete')
  .description('Delete your Monitoring Dashboard')
  .action(async () => {
    const exitCode = await deleteDashboard(DBOSCloudHost);
    process.exit(exitCode);
  });

////////////////////////////
/* ORGANIZATIONS COMMANDS */
////////////////////////////

const orgCommands = program
  .command('organization')
  .alias('organizations')
  .alias('org')
  .description('Manage dbos organizations');

orgCommands
  .command('invite')
  .description('Generate an invite secret for a user to join your organization')
  .option('--json', 'Emit JSON output')
  .action(async (options: { json: boolean }) => {
    const exitCode = await orgInvite(DBOSCloudHost, options.json);
    process.exit(exitCode);
  });

orgCommands
  .command('list')
  .description('List users in your organization')
  .option('--json', 'Emit JSON output')
  .action(async (options: { json: boolean }) => {
    const exitCode = await orgListUsers(DBOSCloudHost, options.json);
    process.exit(exitCode);
  });

orgCommands
  .command('rename')
  .description('Rename the organization')
  .argument('<oldname>', 'Organization old name')
  .argument('<newname>', 'Organization new name')
  .action(async (oldname: string, newname: string) => {
    const exitCode = await renameOrganization(DBOSCloudHost, oldname, newname);
    process.exit(exitCode);
  });

orgCommands
  .command('join')
  .description('Join an organization with an invite secret')
  .argument('<organization>', 'Organization name')
  .argument('<secret>', 'Organization secret')
  .action(async (organization: string, secret: string) => {
    const exitCode = await joinOrganization(DBOSCloudHost, organization, secret);
    process.exit(exitCode);
  });

orgCommands
  .command('remove')
  .description('Remove a user from an organization')
  .argument('<username>', 'User to remove')
  .action(async (username: string) => {
    const exitCode = await removeUserFromOrg(DBOSCloudHost, username);
    process.exit(exitCode);
  });

////////////////////////////
/* WORKFLOW COMMANDS */
////////////////////////////

const workflowCommands = program
  .command('workflow')
  .alias('workflows')
  .alias('wf')
  .description('Manage DBOS workflows');
workflowCommands
  .command('list')
  .description('List workflows from your application')
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('-l, --limit <number>', 'Limit the results returned', '10')
  .option('-o, --offset <number>', 'Skip workflows from the results returned.')
  .option('-u, --workflowUUIDs <uuid...>', 'Retrieve specific UUIDs')
  .option('-U, --user <string>', 'Retrieve workflows run by this user')
  .option('-s, --start-time <string>', 'Retrieve workflows starting after this timestamp (ISO 8601 format)')
  .option('-e, --end-time <string>', 'Retrieve workflows starting before this timestamp (ISO 8601 format)')
  .option(
    '-S, --status <string>',
    'Retrieve workflows with this status (PENDING, SUCCESS, ERROR, RETRIES_EXCEEDED, ENQUEUED, or CANCELLED)',
  )
  .option('-v, --application-version <string>', 'Retrieve workflows with this application version')
  .option('-n, --name <string>', 'Retrieve functions with this name')
  .option('--sort-desc', 'Sort outputs by creation time in descending order (else ascending)')
  .action(
    async (
      appName: string | undefined,
      options: {
        limit?: string;
        user?: string;
        startTime?: string;
        endTime?: string;
        status?: string;
        applicationVersion?: string;
        workflowUUIDs?: string[];
        offset?: string;
        name?: string;
        sortDesc?: boolean;
      },
    ) => {
      const input: ListWorkflowsInput = {
        limit: Number(options.limit),
        workflow_uuids: options.workflowUUIDs,
        authenticated_user: options.user,
        start_time: options.startTime,
        end_time: options.endTime,
        status: options.status,
        application_version: options.applicationVersion,
        offset: Number(options.offset),
        workflow_name: options.name,
        sort_desc: options.sortDesc,
      };
      const exitCode = await listWorkflows(DBOSCloudHost, input, appName);
      process.exit(exitCode);
    },
  );

workflowCommands
  .command('cancel')
  .description('Cancel a workflow so it is no longer automatically retried or restarted')
  .argument('[string]', 'application name (Default: name from package.json)')
  .requiredOption('-w, --workflowid <wfid>', 'The ID of the workflow to cancel')
  .action(
    async (
      appName: string | undefined,
      options: {
        workflowid: string;
      },
    ) => {
      const exitCode = await cancelWorkflow(DBOSCloudHost, options.workflowid, appName);
      process.exit(exitCode);
    },
  );

workflowCommands
  .command('resume')
  .description('Resume a workflow from the last step it executed, keeping its workflow ID')
  .argument('[string]', 'application name (Default: name from package.json)')
  .requiredOption('-w, --workflowid <wfid>', 'The ID of the workflow to resume')
  .action(
    async (
      appName: string | undefined,
      options: {
        workflowid: string;
      },
    ) => {
      const exitCode = await resumeWorkflow(DBOSCloudHost, options.workflowid, appName);
      process.exit(exitCode);
    },
  );

workflowCommands
  .command('restart')
  .description('Restart a workflow from the beginning with a new workflow ID')
  .argument('[string]', 'application name (Default: name from package.json)')
  .requiredOption('-w, --workflowid <wfid>', 'The ID of the workflow to restart')
  .action(
    async (
      appName: string | undefined,
      options: {
        workflowid: string;
      },
    ) => {
      const exitCode = await restartWorkflow(DBOSCloudHost, options.workflowid, appName);
      process.exit(exitCode);
    },
  );

const queueCommands = workflowCommands.command('queue').alias('queues').alias('q').description('Manage DBOS queues');
queueCommands
  .command('list')
  .description('List enqueued functions from your application')
  .argument('[string]', 'application name (Default: name from package.json)')
  .option('-l, --limit <number>', 'Limit the results returned')
  .option('-o, --offset <number>', 'Skip functions from the results returned.')
  .option('-s, --start-time <string>', 'Retrieve functions starting after this timestamp (ISO 8601 format)')
  .option('-e, --end-time <string>', 'Retrieve functions starting before this timestamp (ISO 8601 format)')
  .option('-q, --queue <string>', 'Retrieve functions run on this queue')
  .option('-n, --name <string>', 'Retrieve functions with this name')
  .option(
    '-S, --status <string>',
    'Retrieve functions with this status (PENDING, SUCCESS, ERROR, RETRIES_EXCEEDED, ENQUEUED, or CANCELLED)',
  )
  .option('--sort-desc', 'Sort outputs by creation time in descending order (else ascending)')
  .action(
    async (
      appName: string | undefined,
      options: {
        limit?: string;
        startTime?: string;
        endTime?: string;
        status?: string;
        offset?: string;
        name?: string;
        queue?: string;
        sortDesc?: boolean;
      },
    ) => {
      const input: ListQueuedWorkflowsInput = {
        limit: Number(options.limit),
        start_time: options.startTime,
        end_time: options.endTime,
        status: options.status,
        offset: Number(options.offset),
        workflow_name: options.name,
        queue_name: options.queue,
        sort_desc: options.sortDesc,
      };
      const exitCode = await listQueuedWorkflows(DBOSCloudHost, input, appName);
      process.exit(exitCode);
    },
  );

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
