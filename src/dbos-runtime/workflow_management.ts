import { createLogger } from 'winston';
import { DBOSConfig, GetWorkflowsInput, StatusString } from '..';
import { PostgresSystemDatabase, SystemDatabase } from '../system_database';
import { GlobalLogger } from '../telemetry/logs';
import { GetQueuedWorkflowsInput, WorkflowStatus } from '../workflow';
import { HTTPRequest } from '../context';
import axios from 'axios';

export async function listWorkflows(
  config: DBOSConfig,
  input: GetWorkflowsInput,
  getRequest: boolean,
): Promise<WorkflowInformation[]> {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );
  const workflowUUIDs = (await systemDatabase.getWorkflows(input)).workflowUUIDs;
  const workflowInfos = await Promise.all(
    workflowUUIDs.map(async (i) => await getWorkflowInfo(systemDatabase, i, getRequest)),
  );
  await systemDatabase.destroy();
  return workflowInfos;
}

export async function listQueuedWorkflows(
  config: DBOSConfig,
  input: GetQueuedWorkflowsInput,
  getRequest: boolean,
): Promise<WorkflowInformation[]> {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );
  const workflowUUIDs = (await systemDatabase.getQueuedWorkflows(input)).workflowUUIDs;
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

async function getWorkflowInfo(
  systemDatabase: SystemDatabase,
  workflowUUID: string,
  getRequest: boolean,
): Promise<WorkflowInformation> {
  const info = (await systemDatabase.getWorkflowStatus(workflowUUID)) as WorkflowInformation;
  info.workflowUUID = workflowUUID;
  if (info === null) {
    return Promise.resolve({} as WorkflowInformation);
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

export async function cancelWorkflow(host: string, workflowUUID: string, logger: GlobalLogger) {
  const url = `http://${host}:3001/workflows/${workflowUUID}/cancel`;
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

    logger.info(`Workflow ${workflowUUID} successfully cancelled!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to cancel workflow ${workflowUUID}. Please check that application is running.`;
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
