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
      startedAtEpochMs: step.started_at_epoch_ms,
      completedAtEpochMs: step.completed_at_epoch_ms,
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
    createdAt: internal.createdAt,
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
  };
}

export async function globalTimeout(sysdb: SystemDatabase, cutoffEpochTimestampMs: number): Promise<void> {
  const cutoffIso = new Date(cutoffEpochTimestampMs).toISOString();
  for (const workflow of await listWorkflows(sysdb, { status: 'PENDING', endTime: cutoffIso })) {
    await sysdb.cancelWorkflows([workflow.workflowID]);
  }
  for (const workflow of await listWorkflows(sysdb, { status: 'ENQUEUED', endTime: cutoffIso })) {
    await sysdb.cancelWorkflows([workflow.workflowID]);
  }
  for (const workflow of await listWorkflows(sysdb, { status: 'DELAYED', endTime: cutoffIso })) {
    await sysdb.cancelWorkflows([workflow.workflowID]);
  }
}
