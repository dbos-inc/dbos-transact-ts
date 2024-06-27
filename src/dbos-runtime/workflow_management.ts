import { createLogger } from "winston";
import { DBOSConfig, GetWorkflowsInput, StatusString } from "..";
import { PostgresSystemDatabase, SystemDatabase } from "../system_database";
import { GlobalLogger } from "../telemetry/logs";
import { WorkflowStatus } from "../workflow";
import { HTTPRequest } from "../context";
import { DBOSExecutor } from "../dbos-executor";
import { DBOSRuntime, DBOSRuntimeConfig } from "./runtime";
import { getAllRegisteredClasses } from "../decorators";

export async function listWorkflows(config: DBOSConfig, input: GetWorkflowsInput, getRequest: boolean) {
  const systemDatabase = new PostgresSystemDatabase(config.poolConfig, config.system_database, createLogger() as unknown as GlobalLogger)
  const workflowUUIDs = (await systemDatabase.getWorkflows(input)).workflowUUIDs.reverse(); // Reverse so most recent entries are printed last
  const workflowInfos = await Promise.all(workflowUUIDs.map(async (i) => await getWorkflowInfo(systemDatabase, i, getRequest)))
  await systemDatabase.destroy();
  return workflowInfos;
}

export type WorkflowInformation = Omit<WorkflowStatus, 'request'> & {
  workflowUUID: string,
  input?: unknown[],
  output?: unknown,
  error?: unknown,
  request?: HTTPRequest,
}

async function getWorkflowInfo(systemDatabase: SystemDatabase, workflowUUID: string, getRequest: boolean) {
  const info = await systemDatabase.getWorkflowStatus(workflowUUID) as WorkflowInformation;
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
    const result = await systemDatabase.getWorkflowResult(workflowUUID);
    info.error = result;
  }
  if (!getRequest) {
    delete info.request;
  }
  return info;
}

export async function getWorkflow(config: DBOSConfig, workflowUUID: string, getRequest: boolean) {
  const systemDatabase = new PostgresSystemDatabase(config.poolConfig, config.system_database, createLogger() as unknown as GlobalLogger)
  const info = await getWorkflowInfo(systemDatabase, workflowUUID, getRequest);
  await systemDatabase.destroy();
  return info;
}

// Cancelling a workflow prevents it from being recovered or restarting, but active executions are not halted.
export async function cancelWorkflow(config: DBOSConfig, workflowUUID: string) {
  const systemDatabase = new PostgresSystemDatabase(config.poolConfig, config.system_database, createLogger() as unknown as GlobalLogger)
  await systemDatabase.setWorkflowStatus(workflowUUID, StatusString.CANCELLED)
  await systemDatabase.destroy();
}

export async function retryWorkflow(config: DBOSConfig, runtimeConfig: DBOSRuntimeConfig | null, workflowUUID: string) {
  const dbosExec = new DBOSExecutor(config);
  const classes = runtimeConfig !== null ? await DBOSRuntime.loadClasses(runtimeConfig.entrypoints) : [];
  for (const cls of getAllRegisteredClasses()) {
    if (!classes.includes(cls)) classes.push(cls);
  }
  await dbosExec.init(classes);
  await dbosExec.systemDatabase.setWorkflowStatus(workflowUUID, StatusString.PENDING);
  const handle = await dbosExec.executeWorkflowUUID(workflowUUID);
  await handle.getResult();
  await dbosExec.destroy();
}
