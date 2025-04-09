import { createLogger } from 'winston';
import { GetWorkflowsInput, StatusString } from '..';
import { DBOSConfigInternal, DBOSExecutor } from '../dbos-executor';
import { PostgresSystemDatabase, SystemDatabase, WorkflowStatusInternal } from '../system_database';
import { GlobalLogger } from '../telemetry/logs';
import { GetQueuedWorkflowsInput } from '../workflow';
import { HTTPRequest } from '../context';
import axios from 'axios';
import { DBOS } from '../dbos';

export async function listWorkflows(
  config: DBOSConfigInternal,
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
  config: DBOSConfigInternal,
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

export async function listWorkflowSteps(config: DBOSConfigInternal, workflowUUID: string) {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );
  DBOS.setConfig(config);
  await DBOS.launch();
  const workflowSteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(workflowUUID);
  await DBOS.shutdown();
  return workflowSteps;
}

export type WorkflowInformation = Omit<WorkflowStatusInternal, 'request' | 'error'> & {
  input?: unknown[];
  request?: HTTPRequest;
  error?: unknown;
};

export async function getWorkflowInfo(
  systemDatabase: SystemDatabase,
  workflowUUID: string,
  getRequest: boolean,
): Promise<WorkflowInformation> {
  const info = (await systemDatabase.getWorkflowStatusInternal(workflowUUID)) as WorkflowInformation;
  if (info === null) {
    return Promise.resolve({} as WorkflowInformation);
  }
  delete info.error; // Remove error from info, and add it back if needed
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

export async function getWorkflow(config: DBOSConfigInternal, workflowUUID: string, getRequest: boolean) {
  const systemDatabase = new PostgresSystemDatabase(
    config.poolConfig,
    config.system_database,
    createLogger() as unknown as GlobalLogger,
  );

  const info = await getWorkflowInfo(systemDatabase, workflowUUID, getRequest);
  await systemDatabase.destroy();
  return info;
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
