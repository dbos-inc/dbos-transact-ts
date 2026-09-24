import type { SystemDatabase, WorkflowStatusInternal } from './system_database';
import type { StepInfo, WorkflowStatus, GetWorkflowsInput, ListWorkflowStepsOptions } from './workflow';
import { StatusString } from './workflow';
import type { DataSourceTransactionHandler } from './datasource';
import { DBOSError, DBOSNonExistentWorkflowError } from './error';
import { DBOSSerializer, safeParse, safeParseError, safeParsePositionalArgs } from './serialization';
import { randomUUID } from 'node:crypto';
import type { GlobalLogger } from './telemetry/logs';

export async function listWorkflows(sysdb: SystemDatabase, input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
  const workflows = await sysdb.listWorkflows(input);
  return await Promise.all(workflows.map((wf) => toWorkflowStatus(wf, sysdb.serializer)));
}

export async function listQueuedWorkflows(sysdb: SystemDatabase, input: GetWorkflowsInput) {
  input.queuesOnly = true;
  input.loadOutput = false;
  const workflows = await sysdb.listWorkflows(input);
  return await Promise.all(workflows.map((wf) => toWorkflowStatus(wf, sysdb.serializer)));
}

export async function getWorkflow(sysdb: SystemDatabase, workflowID: string): Promise<WorkflowStatus | undefined> {
  const status = await sysdb.getWorkflowStatus(workflowID);
  return status ? await toWorkflowStatus(status, sysdb.serializer) : undefined;
}

export async function listWorkflowSteps(
  sysdb: SystemDatabase,
  workflowID: string,
  loadOutput: boolean = true,
  options?: ListWorkflowStepsOptions,
): Promise<StepInfo[] | undefined> {
  const status = await sysdb.getWorkflowStatus(workflowID);
  if (!status) {
    return undefined;
  }

  const $steps = await sysdb.getAllOperationResults(workflowID, options?.limit, options?.offset);

  const steps: StepInfo[] = await Promise.all(
    $steps.map(async (step) => ({
      functionID: step.function_id,
      name: step.function_name ?? '',
      output: loadOutput && step.output ? await safeParse(sysdb.serializer, step.output, step.serialization) : null,
      error: loadOutput && step.error ? await safeParseError(sysdb.serializer, step.error, step.serialization) : null,
      childWorkflowID: step.child_workflow_id,
      startedAtEpochMs: step.started_at_epoch_ms ? Number(step.started_at_epoch_ms) : undefined,
      completedAtEpochMs: step.completed_at_epoch_ms ? Number(step.completed_at_epoch_ms) : undefined,
    })),
  );

  return steps.toSorted((a, b) => a.functionID - b.functionID);
}

export async function forkWorkflow(
  sysdb: SystemDatabase,
  workflowID: string,
  startStep: number,
  options: {
    newWorkflowID?: string;
    applicationVersion?: string;
    timeoutMS?: number;
    queueName?: string;
    queuePartitionKey?: string;
    replacementChildren?: Record<string, string>;
  } = {},
): Promise<string> {
  const newWorkflowID = options.newWorkflowID ?? randomUUID();
  await sysdb.forkWorkflow(workflowID, startStep, { ...options, newWorkflowID });
  return newWorkflowID;
}

/**
 * Refuse to touch a data source's checkpoints for a workflow that is missing or still
 * running. The system database rewind repeats this check under its own transaction;
 * this one only keeps a running workflow's checkpoints intact.
 */
async function checkRewindable(sysdb: SystemDatabase, workflowID: string): Promise<void> {
  const status = await sysdb.getWorkflowStatus(workflowID);
  if (status === null) {
    throw new DBOSNonExistentWorkflowError(`Workflow ${workflowID} does not exist`);
  }
  if (
    status.status === StatusString.PENDING ||
    status.status === StatusString.ENQUEUED ||
    status.status === StatusString.DELAYED
  ) {
    throw new DBOSError(
      `Cannot rewind ${workflowID} (${status.status}): only a workflow in a terminal state can be rewound, so cancel it first`,
    );
  }
}

/**
 * Drop the data sources' checkpoints from `startStep` on, then rewind the workflow in
 * the system database.
 *
 * Best effort: if a step fails the workflow is left as it was and the rewind can be
 * retried, which is safe because the deletes are idempotent and the system database is
 * only touched last. A data source that keeps no checkpoints of its own does not
 * implement `deleteCheckpoints` and is skipped.
 */
export async function rewindWorkflow(
  sysdb: SystemDatabase,
  dataSources: readonly DataSourceTransactionHandler[],
  workflowID: string,
  startStep: number,
  options: {
    applicationVersion?: string;
    queueName?: string;
    queuePartitionKey?: string;
  } = {},
): Promise<void> {
  if (startStep < 0) {
    throw new DBOSError(`startStep must be >= 0, got ${startStep}`);
  }
  const withCheckpoints = keepingCheckpoints(dataSources);
  // Deleting a running workflow's checkpoints would pull them out from under the
  // execution that still owns them, so establish the workflow is rewindable before
  // touching anything the system database rewind will not re-check for us.
  if (withCheckpoints.length > 0) {
    await checkRewindable(sysdb, workflowID);
  }
  for (const ds of withCheckpoints) {
    await ds.deleteCheckpoints!(workflowID, startStep);
  }
  await sysdb.rewindWorkflow(workflowID, startStep, options);
}

/** The data sources that keep checkpoints of their own; the others do not implement `deleteCheckpoints`. */
function keepingCheckpoints(dataSources: readonly DataSourceTransactionHandler[]) {
  return dataSources.filter((ds) => ds.deleteCheckpoints !== undefined);
}

/**
 * Drop a finishing workflow's data source checkpoints, which its step checkpoints now cover.
 *
 * Best effort: a leftover checkpoint is harmless, so a failure only warns and the other
 * data sources are still cleared.
 */
export async function deleteCompletedDataSourceCheckpoints(
  dataSources: readonly DataSourceTransactionHandler[],
  workflowID: string,
  logger: GlobalLogger,
): Promise<void> {
  for (const ds of keepingCheckpoints(dataSources)) {
    try {
      await ds.deleteCheckpoints!(workflowID, 0);
    } catch (e) {
      logger.warn(
        `Failed to delete data source ${ds.name} checkpoints of workflow ${workflowID}: ${(e as Error).message}`,
      );
    }
  }
}

export async function toWorkflowStatus(
  internal: WorkflowStatusInternal,
  serializer: DBOSSerializer,
): Promise<WorkflowStatus> {
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

    input: internal.input
      ? ((await safeParsePositionalArgs(serializer, internal.input, internal.serialization)) as unknown[])
      : undefined,
    output: internal.output ? await safeParse(serializer, internal.output ?? null, internal.serialization) : undefined,
    error: internal.error ? await safeParseError(serializer, internal.error, internal.serialization) : undefined,

    executorId: internal.executorId,
    applicationVersion: internal.applicationVersion,
    recoveryAttempts: internal.recoveryAttempts,
    createdAt: internal.createdAt!,
    updatedAt: internal.updatedAt,
    timeoutMS: internal.timeoutMS,
    deadlineEpochMS: internal.deadlineEpochMS,
    deduplicationID: internal.deduplicationID,
    priority: internal.priority,
    queuePartitionKey: internal.queuePartitionKey,
    dequeuedAt: internal.startedAtEpochMs,
    forkedFrom: internal.forkedFrom,
    wasForkedFrom: internal.wasForkedFrom ?? false,
    parentWorkflowID: internal.parentWorkflowID,
    delayUntilEpochMS: internal.delayUntilEpochMS,
    completedAt: internal.completedAt,
    attributes: internal.attributes,
    scheduleName: internal.scheduleName,
    applicationName: internal.applicationName,
  };
}

/** Enforce retention across the entire system database. */
export async function garbageCollect(
  sysdb: SystemDatabase,
  cutoffEpochTimestampMs?: number | null,
  rowsThreshold?: number | null,
  options: { batchSize?: number | null } = {},
): Promise<void> {
  if (
    (cutoffEpochTimestampMs === undefined || cutoffEpochTimestampMs === null) &&
    (rowsThreshold === undefined || rowsThreshold === null)
  ) {
    return;
  }
  const lock = await sysdb.acquireRetentionLock();
  if (!lock) {
    sysdb.logger.warn('Skipping retention: another round is already running against this system database.');
    return;
  }
  try {
    // Both sweeps take the same batch size, and both default it the same way when unset.
    const batchSize = options.batchSize ?? undefined;
    const cutoff = await sysdb.garbageCollect(cutoffEpochTimestampMs, rowsThreshold, { batchSize });
    if (cutoff === undefined) {
      return;
    }
    // Strictly after the status sweep: the payload sweep only takes orphans, so this round's
    // are only visible to it once that sweep has committed.
    await sysdb.garbageCollectPayloads(cutoff, batchSize);
  } finally {
    await lock.release();
  }
}

export async function globalTimeout(sysdb: SystemDatabase, cutoffEpochTimestampMs: number): Promise<void> {
  // IDs only, so a bulk timeout does not deserialize every row's inputs and outputs.
  for (const workflowID of await sysdb.listTimedOutWorkflowIds(cutoffEpochTimestampMs)) {
    await sysdb.cancelWorkflows([workflowID]);
  }
}

export const workflowTimeoutConfig = {
  /** How often the sweep looks for workflows past their deadline. */
  pollingIntervalMs: 1000,
  /** Most workflows one sweep transaction cancels; a full batch sweeps again at once. */
  batchSize: 1000,
};

function waitOrAbort(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const onAbort = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

/** Cancel this application's active workflows once their deadline passes, until `signal` aborts. */
export async function workflowTimeoutLoop(
  sysdb: SystemDatabase,
  logger: GlobalLogger,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    try {
      while (!signal.aborted) {
        const cancelled = await sysdb.cancelTimedOutWorkflows(workflowTimeoutConfig.batchSize);
        for (const workflowID of cancelled) {
          logger.debug(`Cancelled workflow ${workflowID}: timed out`);
        }
        if (cancelled.length < workflowTimeoutConfig.batchSize) break;
      }
    } catch (e) {
      logger.warn(`Exception cancelling timed-out workflows: ${(e as Error).message}`);
    }
    await waitOrAbort(workflowTimeoutConfig.pollingIntervalMs, signal);
  }
}
