import type { SystemDatabase, WorkflowStatusInternal } from './system_database';
import type { StepInfo, WorkflowStatus, GetWorkflowsInput, ListWorkflowStepsOptions } from './workflow';
import { DBOSSerializer, safeParse, safeParseError, safeParsePositionalArgs } from './serialization';
import { randomUUID } from 'node:crypto';

export async function listWorkflows(sysdb: SystemDatabase, input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
  const workflows = await sysdb.listWorkflows(input);
  return await Promise.all(workflows.map((wf) => toWorkflowStatus(wf, sysdb.getSerializer())));
}

export async function listQueuedWorkflows(sysdb: SystemDatabase, input: GetWorkflowsInput) {
  input.queuesOnly = true;
  input.loadOutput = false;
  const workflows = await sysdb.listWorkflows(input);
  return await Promise.all(workflows.map((wf) => toWorkflowStatus(wf, sysdb.getSerializer())));
}

export async function getWorkflow(sysdb: SystemDatabase, workflowID: string): Promise<WorkflowStatus | undefined> {
  const status = await sysdb.getWorkflowStatus(workflowID);
  return status ? await toWorkflowStatus(status, sysdb.getSerializer()) : undefined;
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
      output:
        loadOutput && step.output ? await safeParse(sysdb.getSerializer(), step.output, step.serialization) : null,
      error:
        loadOutput && step.error ? await safeParseError(sysdb.getSerializer(), step.error, step.serialization) : null,
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

    request: internal.request,
    executorId: internal.executorId,
    applicationVersion: internal.applicationVersion,
    applicationID: internal.applicationID,
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
