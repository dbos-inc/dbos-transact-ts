import { createLogger } from "winston";
import { DBOSConfig, GetWorkflowsInput, StatusString } from "..";
import { PostgresSystemDatabase } from "../system_database";
import { GlobalLogger } from "../telemetry/logs";
import { WorkflowStatus } from "../workflow";

export async function listWorkflows(config: DBOSConfig, input: GetWorkflowsInput) {
  const systemDatabase = new PostgresSystemDatabase(config.poolConfig, config.system_database, createLogger() as unknown as GlobalLogger)
  const workflows = await systemDatabase.getWorkflows(input);
  await systemDatabase.destroy();
  return workflows;
}

export type WorkflowInformation = WorkflowStatus & {
  input?: unknown[],
  output?: unknown,
  error?: unknown,
}

export async function getWorkflow(config: DBOSConfig, workflowUUID: string) {
  const systemDatabase = new PostgresSystemDatabase(config.poolConfig, config.system_database, createLogger() as unknown as GlobalLogger)
  const info = await systemDatabase.getWorkflowStatus(workflowUUID) as WorkflowInformation;
  if (info === null) {
    return {}
  }
  const input = await systemDatabase.getWorkflowInputs(workflowUUID);
  if (input !== null) {
    info.input = input
  }
  if (info.status === StatusString.SUCCESS) {
    const result = await systemDatabase.getWorkflowResult(workflowUUID);
    info.output = result;
  } else if (info.status === StatusString.ERROR) {
    const result = await systemDatabase.getWorkflowResult(workflowUUID);
    info.error = result;
  }
  await systemDatabase.destroy();
  return info;
}
