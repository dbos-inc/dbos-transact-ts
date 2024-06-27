import { createLogger } from "winston";
import { DBOSConfig, GetWorkflowsInput } from "..";
import { PostgresSystemDatabase } from "../system_database";
import { GlobalLogger } from "../telemetry/logs";

export async function listWorkflows(config: DBOSConfig, input: GetWorkflowsInput) {
  const systemDatabase = new PostgresSystemDatabase(config.poolConfig, config.system_database, createLogger() as unknown as GlobalLogger)
  const workflows = await systemDatabase.getWorkflows(input);
  await systemDatabase.destroy();
  return workflows;
}
