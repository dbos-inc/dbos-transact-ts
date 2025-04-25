import { createLogger } from 'winston';
import { GetWorkflowsInput } from '..';
import { DBOSConfigInternal, DBOSExecutor } from '../dbos-executor';
import { PostgresSystemDatabase, SystemDatabase } from '../system_database';
import { GlobalLogger } from '../telemetry/logs';
import { GetQueuedWorkflowsInput, WorkflowStatus } from '../workflow';
import axios from 'axios';
import { DBOS } from '../dbos';

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
