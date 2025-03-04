import axios, { AxiosError } from 'axios';
import {
  handleAPIErrors,
  getCloudCredentials,
  getLogger,
  isCloudAPIErrorResponse,
  retrieveApplicationName,
} from '../cloudutils.js';

export interface ListWorkflowsInput {
  workflow_uuids?: string[]; // Specific workflow UUIDs to retrieve.
  workflow_name?: string; // The name of the workflow function
  authenticated_user?: string; // The user who ran the workflow.
  start_time?: string; // Timestamp in ISO 8601 format
  end_time?: string; // Timestamp in ISO 8601 format
  status?: string; // The status of the workflow.
  application_version?: string; // The application version that ran this workflow.
  limit?: number; // Return up to this many workflows IDs. IDs are ordered by workflow creation time.
  offset?: number; // Skip this many workflows IDs. IDs are ordered by workflow creation time.
  sort_desc?: boolean; // Sort in DESC order by created_at (default ASC)
}

export interface ListQueuedWorkflowsInput {
  workflow_name?: string; // The name of the workflow function
  queue_name?: string; // The name of the queue
  start_time?: string; // Timestamp in ISO 8601 format
  end_time?: string; // Timestamp in ISO 8601 format
  status?: string; // The status of the workflow.
  limit?: number; // Return up to this many workflows IDs. IDs are ordered by workflow creation time.
  offset?: number; // Skip this many workflows IDs. IDs are ordered by workflow creation time.
  sort_desc?: boolean; // Sort in DESC order by created_at (default ASC)
}

export async function listWorkflows(host: string, input: ListWorkflowsInput, appName?: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  appName = appName ?? retrieveApplicationName(logger, true);
  if (!appName) {
    return 1;
  }

  try {
    const res = await axios.post(
      `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}/workflows`,
      input,
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );
    console.log(JSON.stringify(res.data));
    return 0;
  } catch (e) {
    const errorLabel = `Failed to list workflows for application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function listQueuedWorkflows(
  host: string,
  input: ListQueuedWorkflowsInput,
  appName?: string,
): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  appName = appName ?? retrieveApplicationName(logger, true);
  if (!appName) {
    return 1;
  }

  try {
    const res = await axios.post(
      `https://${host}/v1alpha1/${userCredentials.organization}/applications/${appName}/queues`,
      input,
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );
    console.log(JSON.stringify(res.data));
    return 0;
  } catch (e) {
    const errorLabel = `Failed to list workflows for application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function cancelWorkflow(host: string, workflowID: string, appName?: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  appName = appName ?? retrieveApplicationName(logger, true);
  if (!appName) {
    return 1;
  }

  try {
    await axios.post(
      `https://${host}/appsadmin/${userCredentials.organization}/applications/${appName}/workflows/${workflowID}/cancel`,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );
    return 0;
  } catch (e) {
    const errorLabel = `Failed to cancel workflow ${workflowID} in application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function resumeWorkflow(host: string, workflowID: string, appName?: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  appName = appName ?? retrieveApplicationName(logger, true);
  if (!appName) {
    return 1;
  }

  try {
    await axios.post(
      `https://${host}/appsadmin/${userCredentials.organization}/applications/${appName}/workflows/${workflowID}/resume`,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );
    return 0;
  } catch (e) {
    const errorLabel = `Failed to resume workflow ${workflowID} in application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function restartWorkflow(host: string, workflowID: string, appName?: string): Promise<number> {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  appName = appName ?? retrieveApplicationName(logger, true);
  if (!appName) {
    return 1;
  }

  try {
    await axios.post(
      `https://${host}/appsadmin/${userCredentials.organization}/applications/${appName}/workflows/${workflowID}/restart`,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );
    return 0;
  } catch (e) {
    const errorLabel = `Failed to restart workflow ${workflowID} in application ${appName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}
