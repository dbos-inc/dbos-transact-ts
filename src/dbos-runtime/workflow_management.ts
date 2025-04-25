import { createLogger } from 'winston';
import { GetWorkflowsInput } from '..';
import { DBOSConfigInternal, DBOSExecutor } from '../dbos-executor';
import { PostgresSystemDatabase, SystemDatabase, WorkflowStatusInternal } from '../system_database';
import { GlobalLogger } from '../telemetry/logs';
import { GetQueuedWorkflowsInput, WorkflowStatus } from '../workflow';
import axios from 'axios';
import { DBOS } from '../dbos';
import { UserDatabase } from '../user_database';
import { DBOSJSON } from '../utils';
import { deserializeError } from 'serialize-error';
import { transaction_outputs } from '../../schemas/user_db_schema';
import { v4 as uuidv4 } from 'uuid';

function $toWorkflowStatus(internal: WorkflowStatusInternal, getRequest: boolean = true): WorkflowStatus {
  return {
    workflowID: internal.workflowUUID,
    status: internal.status,
    workflowName: internal.workflowName,
    workflowClassName: internal.workflowClassName,
    workflowConfigName: internal.workflowConfigName,
    queueName: internal.queueName,

    authenticatedUser: internal.authenticatedUser,
    assumedRole: internal.assumedRole,
    authenticatedRoles: internal.authenticatedRoles,

    input: internal.input ? (DBOSJSON.parse(internal.input) as unknown[]) : undefined,
    output: internal.output ? DBOSJSON.parse(internal.output ?? null) : undefined,
    error: internal.error ? deserializeError(DBOSJSON.parse(internal.error)) : undefined,

    request: getRequest ? internal.request : undefined,
    executorId: internal.executorId,
    applicationVersion: internal.applicationVersion,
    applicationID: internal.applicationID,
    recoveryAttempts: internal.recoveryAttempts,
    createdAt: internal.createdAt,
    updatedAt: internal.updatedAt,
  };
}

async function $listWorkflows(
  sysdb: SystemDatabase,
  input: GetWorkflowsInput,
  getRequest: boolean = true,
): Promise<WorkflowStatus[]> {
  const workflows = await sysdb.listWorkflows(input);
  return workflows.map((wf) => $toWorkflowStatus(wf, getRequest));
}

async function $listQueuedWorkflows(sysdb: SystemDatabase, input: GetQueuedWorkflowsInput, getRequest: boolean = true) {
  const workflows = await sysdb.listQueuedWorkflows(input);
  return workflows.map((wf) => $toWorkflowStatus(wf, getRequest));
}

async function $getWorkflow(
  sysdb: SystemDatabase,
  workflowID: string,
  getRequest: boolean = true,
): Promise<WorkflowStatus | undefined> {
  const statuses = await sysdb.listWorkflows({ workflowIDs: [workflowID] });
  const status = statuses.find((status) => status.workflowUUID === workflowID);
  return status ? $toWorkflowStatus(status, getRequest) : undefined;
}

interface StepInfo {
  readonly functionID: number;
  readonly name: string;
  readonly output: unknown;
  readonly error: Error | null;
  readonly childWorkflowID: string | null;
}

async function $listWorkflowSteps(
  sysdb: SystemDatabase,
  userdb: UserDatabase,
  workflowID: string,
): Promise<StepInfo[]> {
  const [$steps, $txs] = await Promise.all([
    sysdb.getAllOperationResults(workflowID),
    await userdb.query<transaction_outputs, [string]>(
      `SELECT function_id, function_name, output, error FROM ${DBOSExecutor.systemDBSchemaName}.transaction_outputs 
      WHERE workflow_uuid=$1`,
      workflowID,
    ),
  ]);

  const steps = $steps.map(
    (step) =>
      ({
        functionID: step.function_id,
        name: step.function_name,
        output: step.output ? DBOSJSON.parse(step.output) : null,
        error: step.error ? deserializeError(DBOSJSON.parse(step.error)) : null,
        childWorkflowID: step.child_workflow_id,
      }) as StepInfo,
  );
  const txs = $txs.map((row) => {
    return {
      functionID: row.function_id,
      name: row.function_name,
      output: row.output ? DBOSJSON.parse(row.output) : null,
      error: row.error ? deserializeError(DBOSJSON.parse(row.error)) : null,
      childWorkflowID: null,
    } as StepInfo;
  });

  const merged = [...steps, ...txs];
  merged.sort((a, b) => a.functionID - b.functionID);
  return merged;
}

async function $cancelWorkflow(sysdb: SystemDatabase, workflowID: string) {
  await sysdb.cancelWorkflow(workflowID);
}

async function $resumeWorkflow(sysdb: SystemDatabase, workflowID: string) {
  await sysdb.resumeWorkflow(workflowID);
}

async function $forkWorkflow(sysdb: SystemDatabase, workflowID: string, newWorkflowID?: string) {
  newWorkflowID ??= uuidv4();
  await sysdb.forkWorkflow(workflowID, newWorkflowID);
}

export async function listWorkflows(
  config: DBOSConfigInternal,
  input: GetWorkflowsInput,
  getRequest: boolean,
): Promise<WorkflowStatus[]> {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );
  try {
    const workflows = await systemDatabase.listWorkflows(input);
    return workflows.map((wf) => DBOSExecutor.toWorkflowStatus(wf, getRequest));
  } finally {
    await systemDatabase.destroy();
  }
}

export async function listQueuedWorkflows(
  config: DBOSConfigInternal,
  input: GetQueuedWorkflowsInput,
  getRequest: boolean,
): Promise<WorkflowStatus[]> {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );

  try {
    const workflows = await systemDatabase.listQueuedWorkflows(input);
    return workflows.map((wf) => DBOSExecutor.toWorkflowStatus(wf, getRequest));
  } finally {
    await systemDatabase.destroy();
  }
}

export async function listWorkflowSteps(config: DBOSConfigInternal, workflowUUID: string) {
  DBOS.setConfig(config);
  await DBOS.launch();
  const workflowSteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(workflowUUID);
  await DBOS.shutdown();
  return workflowSteps;
}

export async function getWorkflowInfo(
  systemDatabase: SystemDatabase,
  workflowID: string,
  getRequest: boolean,
): Promise<WorkflowStatus | undefined> {
  const statuses = await systemDatabase.listWorkflows({ workflowIDs: [workflowID] });
  const status = statuses.find((s) => s.workflowUUID === workflowID);
  return status ? DBOSExecutor.toWorkflowStatus(status, getRequest) : undefined;
}

export async function getWorkflow(config: DBOSConfigInternal, workflowID: string, getRequest: boolean) {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );
  try {
    return await getWorkflowInfo(systemDatabase, workflowID, getRequest);
  } finally {
    await systemDatabase.destroy();
  }
}

export async function cancelWorkflow(host: string, workflowID: string, logger: GlobalLogger) {
  const url = `http://${host}:3001/workflows/${workflowID}/cancel`;
  try {
    const res = await axios.post(
      url,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
        },
      },
    );

    if (res.status !== 204) {
      logger.error(`Failed to cancel ${workflowID}`);
      return 1;
    }

    logger.info(`Workflow ${workflowID} successfully cancelled!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to cancel workflow ${workflowID}. Please check that application is running.`;
    logger.error(`${errorLabel}: ${(e as Error).message}`);

    return 1;
  }
}

export async function resumeWorkflow(host: string, workflowUUID: string, logger: GlobalLogger) {
  const url = `http://${host}:3001/workflows/${workflowUUID}/resume`;
  try {
    const res = await axios.post(
      url,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
        },
      },
    );

    if (res.status !== 204) {
      logger.error(`Failed to cancel ${workflowUUID}`);
      return 1;
    }

    logger.info(`Workflow ${workflowUUID} successfully resume!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to resume workflow ${workflowUUID}. Please check that application is running.`;

    logger.error(`${errorLabel}: ${(e as Error).message}`);

    return 1;
  }
}

export async function restartWorkflow(host: string, workflowUUID: string, logger: GlobalLogger) {
  const url = `http://${host}:3001/workflows/${workflowUUID}/restart`;
  try {
    const res = await axios.post(
      url,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
        },
      },
    );

    if (res.status !== 204) {
      logger.error(`Failed to restart ${workflowUUID}`);
      return 1;
    }

    logger.info(`Workflow ${workflowUUID} successfully restart!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to restart workflow ${workflowUUID}. Please check that application is running.`;

    logger.error(`${errorLabel}: ${(e as Error).message}`);

    return 1;
  }
}
