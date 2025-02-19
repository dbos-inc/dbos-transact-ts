import { createLogger } from 'winston';
import { DBOSConfig, GetWorkflowsInput, StatusString } from '..';
import { PostgresSystemDatabase, SystemDatabase } from '../system_database';
import { GlobalLogger } from '../telemetry/logs';
import { GetQueuedWorkflowsInput, WorkflowStatus } from '../workflow';
import { HTTPRequest } from '../context';
import { DBOSExecutor } from '../dbos-executor';
import { DBOSRuntime, DBOSRuntimeConfig } from './runtime';

export async function listWorkflows(config: DBOSConfig, input: GetWorkflowsInput, getRequest: boolean) {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );
  const workflowUUIDs = (await systemDatabase.getWorkflows(input)).workflowUUIDs.reverse(); // Reverse so most recent entries are printed last
  const workflowInfos = await Promise.all(
    workflowUUIDs.map(async (i) => await getWorkflowInfo(systemDatabase, i, getRequest)),
  );
  await systemDatabase.destroy();
  return workflowInfos;
}

export async function listQueuedWorkflows(config: DBOSConfig, input: GetQueuedWorkflowsInput, getRequest: boolean) {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );
  const workflowUUIDs = (await systemDatabase.getQueuedWorkflows(input)).workflowUUIDs.reverse(); // Reverse so most recent entries are printed last
  const workflowInfos = await Promise.all(
    workflowUUIDs.map(async (i) => await getWorkflowInfo(systemDatabase, i, getRequest)),
  );
  await systemDatabase.destroy();
  return workflowInfos;
}

export type WorkflowInformation = Omit<WorkflowStatus, 'request'> & {
  workflowUUID: string;
  input?: unknown[];
  output?: unknown;
  error?: unknown;
  request?: HTTPRequest;
};

async function getWorkflowInfo(systemDatabase: SystemDatabase, workflowUUID: string, getRequest: boolean) {
  const info = (await systemDatabase.getWorkflowStatus(workflowUUID)) as WorkflowInformation;
  info.workflowUUID = workflowUUID;
  if (info === null) {
    return {};
  }
  const input = await systemDatabase.getWorkflowInputs(workflowUUID);
  if (input !== null) {
    info.input = input;
  }
  if (info.status === StatusString.SUCCESS) {
    const result = await systemDatabase.getWorkflowResult(workflowUUID);
    info.output = result;
  } else if (info.status === StatusString.ERROR) {
    try {
      await systemDatabase.getWorkflowResult(workflowUUID);
    } catch (e) {
      info.error = e;
    }
  }
  if (!getRequest) {
    delete info.request;
  }
  return info;
}

export async function getWorkflow(config: DBOSConfig, workflowUUID: string, getRequest: boolean) {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );
  try {
    const info = await getWorkflowInfo(systemDatabase, workflowUUID, getRequest);
    return info;
  } finally {
    await systemDatabase.destroy();
  }
}

// Cancelling a workflow prevents it from being automatically recovered, but active executions are not halted.
export async function cancelWorkflow(config: DBOSConfig, workflowUUID: string) {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );
  try {
    await systemDatabase.cancelWorkflow(workflowUUID);
  } finally {
    await systemDatabase.destroy();
  }
}

export async function reattemptWorkflow(
  config: DBOSConfig,
  runtimeConfig: DBOSRuntimeConfig | null,
  workflowUUID: string,
  startNewWorkflow: boolean,
) {
  const dbosExec = new DBOSExecutor(config);

  if (runtimeConfig !== null) {
    await DBOSRuntime.loadClasses(runtimeConfig.entrypoints);
  }
  try {
    // await dbosExec.init();
    if (!startNewWorkflow) {
      await dbosExec.systemDatabase.resumeWorkflow(workflowUUID);
    }
    const handle = await dbosExec.executeWorkflowUUID(workflowUUID, startNewWorkflow);
    const output = await handle.getResult();
    return output;
  } finally {
    await dbosExec.destroy();
  }
}
