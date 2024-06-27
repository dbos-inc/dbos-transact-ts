import { createLogger } from "winston";
import { DBOSConfig, GetWorkflowsInput, StatusString } from "..";
import { PostgresSystemDatabase, SystemDatabase } from "../system_database";
import { GlobalLogger } from "../telemetry/logs";
import { WorkflowStatus } from "../workflow";

export async function listWorkflows(config: DBOSConfig, input: GetWorkflowsInput) {
  const systemDatabase = new PostgresSystemDatabase(config.poolConfig, config.system_database, createLogger() as unknown as GlobalLogger)
  const workflowUUIDs = (await systemDatabase.getWorkflows(input)).workflowUUIDs;
  const workflowInfos = await Promise.all(workflowUUIDs.map(async (i) => await getWorkflowInfo(systemDatabase, i)))
  await systemDatabase.destroy();
  return workflowInfos;
}

export type WorkflowInformation = WorkflowStatus & {
  workflowUUID: string,
  input?: unknown[],
  output?: unknown,
  error?: unknown,
}

export async function getWorkflowInfo(systemDatabase: SystemDatabase, workflowUUID: string) {
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
  return info;
}

export async function getWorkflow(config: DBOSConfig, workflowUUID: string) {
  const systemDatabase = new PostgresSystemDatabase(config.poolConfig, config.system_database, createLogger() as unknown as GlobalLogger)
  const info = await getWorkflowInfo(systemDatabase, workflowUUID);
  await systemDatabase.destroy();
  return info;
}
