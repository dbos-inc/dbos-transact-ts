import type { SystemDatabase, WorkflowStatusInternal } from './system_database';
import type { StepInfo, WorkflowStatus, GetWorkflowsInput } from './workflow';
import { DBOSJSON } from './utils';
import { deserializeError } from 'serialize-error';
import { randomUUID } from 'node:crypto';

export async function listWorkflows(sysdb: SystemDatabase, input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
  const workflows = await sysdb.listWorkflows(input);
  return workflows.map((wf) => toWorkflowStatus(wf));
}

export async function listQueuedWorkflows(sysdb: SystemDatabase, input: GetWorkflowsInput) {
  input.queuesOnly = true;
  input.loadOutput = false;
  const workflows = await sysdb.listWorkflows(input);
  return workflows.map((wf) => toWorkflowStatus(wf));
}

export async function getWorkflow(sysdb: SystemDatabase, workflowID: string): Promise<WorkflowStatus | undefined> {
  const status = await sysdb.getWorkflowStatus(workflowID);
  return status ? toWorkflowStatus(status) : undefined;
}

export async function listWorkflowSteps(sysdb: SystemDatabase, workflowID: string): Promise<StepInfo[] | undefined> {
  const status = await sysdb.getWorkflowStatus(workflowID);
  if (!status) {
    return undefined;
  }

  const $steps = await sysdb.getAllOperationResults(workflowID);

  const steps: StepInfo[] = $steps.map((step) => ({
    functionID: step.function_id,
    name: step.function_name ?? '',
    output: step.output ? DBOSJSON.parse(step.output) : null,
    error: step.error ? deserializeError(DBOSJSON.parse(step.error)) : null,
    childWorkflowID: step.child_workflow_id,
  }));

  return steps.toSorted((a, b) => a.functionID - b.functionID);
}

export async function forkWorkflow(
  sysdb: SystemDatabase,
  workflowID: string,
  startStep: number,
  options: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number } = {},
): Promise<string> {
  const newWorkflowID = options.newWorkflowID ?? randomUUID();
  await sysdb.forkWorkflow(workflowID, startStep, { ...options, newWorkflowID });
  return newWorkflowID;
}

export function toWorkflowStatus(internal: WorkflowStatusInternal): WorkflowStatus {
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

    input: internal.input ? (DBOSJSON.parse(internal.input) as unknown[]) : undefined,
    output: internal.output ? DBOSJSON.parse(internal.output ?? null) : undefined,
    error: internal.error ? deserializeError(DBOSJSON.parse(internal.error)) : undefined,

    request: internal.request,
    executorId: internal.executorId,
    applicationVersion: internal.applicationVersion,
    applicationID: internal.applicationID,
    recoveryAttempts: internal.recoveryAttempts,
    createdAt: internal.createdAt,
    updatedAt: internal.updatedAt,
    timeoutMS: internal.timeoutMS,
    deadlineEpochMS: internal.deadlineEpochMS,
  };
}

export async function globalTimeout(sysdb: SystemDatabase, cutoffEpochTimestampMs: number): Promise<void> {
  const cutoffIso = new Date(cutoffEpochTimestampMs).toISOString();
  for (const workflow of await listWorkflows(sysdb, { status: 'PENDING', endTime: cutoffIso })) {
    await sysdb.cancelWorkflow(workflow.workflowID);
  }
  for (const workflow of await listWorkflows(sysdb, { status: 'ENQUEUED', endTime: cutoffIso })) {
    await sysdb.cancelWorkflow(workflow.workflowID);
  }
}
