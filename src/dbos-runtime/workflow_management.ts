import type { GetWorkflowsInput } from '..';
import { DBOSExecutor } from '../dbos-executor';
import type { SystemDatabase, WorkflowStatusInternal } from '../system_database';
import type { GetQueuedWorkflowsInput, StepInfo, WorkflowStatus } from '../workflow';
import type { UserDatabase } from '../user_database';
import { DBOSJSON } from '../utils';
import { deserializeError } from 'serialize-error';
import type { transaction_outputs } from '../../schemas/user_db_schema';
import { randomUUID } from 'node:crypto';

export async function listWorkflows(sysdb: SystemDatabase, input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
  const workflows = await sysdb.listWorkflows(input);
  return workflows.map((wf) => toWorkflowStatus(wf));
}

export async function listQueuedWorkflows(sysdb: SystemDatabase, input: GetQueuedWorkflowsInput) {
  const workflows = await sysdb.listQueuedWorkflows(input);
  return workflows.map((wf) => toWorkflowStatus(wf));
}

export async function getWorkflow(sysdb: SystemDatabase, workflowID: string): Promise<WorkflowStatus | undefined> {
  const status = await sysdb.getWorkflowStatus(workflowID);
  return status ? toWorkflowStatus(status) : undefined;
}

export async function listWorkflowSteps(
  sysdb: SystemDatabase,
  userdb: UserDatabase,
  workflowID: string,
): Promise<StepInfo[] | undefined> {
  const status = await sysdb.getWorkflowStatus(workflowID);
  if (!status) {
    return undefined;
  }

  type TxOutputs = Pick<transaction_outputs, 'function_id' | 'function_name' | 'output' | 'error'>;
  const [$steps, $txs] = await Promise.all([
    sysdb.getAllOperationResults(workflowID),
    await userdb.query<TxOutputs, [string]>(
      `SELECT function_id, function_name, output, error FROM ${DBOSExecutor.systemDBSchemaName}.transaction_outputs 
      WHERE workflow_uuid=$1`,
      workflowID,
    ),
  ]);

  const steps: StepInfo[] = $steps.map((step) => ({
    functionID: step.function_id,
    name: step.function_name ?? '',
    //eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    output: step.output ? DBOSJSON.parse(step.output) : null,
    error: step.error ? deserializeError(DBOSJSON.parse(step.error)) : null,
    childWorkflowID: step.child_workflow_id,
  }));
  const txs: StepInfo[] = $txs.map((row) => ({
    functionID: row.function_id,
    name: row.function_name,
    //eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    output: row.output ? DBOSJSON.parse(row.output) : null,
    error: row.error ? deserializeError(DBOSJSON.parse(row.error)) : null,
    childWorkflowID: null,
  }));

  return [...steps, ...txs].toSorted((a, b) => a.functionID - b.functionID);
}

export async function forkWorkflow(
  sysdb: SystemDatabase,
  userdb: UserDatabase,
  workflowID: string,
  startStep: number,
  options: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number } = {},
): Promise<string> {
  const newWorkflowID = options.newWorkflowID ?? randomUUID();
  const query = `
    INSERT INTO dbos.transaction_outputs
      (workflow_uuid, function_id, output, error, txn_id, txn_snapshot, function_name)
    SELECT $1 AS workflow_uuid, function_id, output, error, txn_id, txn_snapshot, function_name
      FROM dbos.transaction_outputs 
      WHERE workflow_uuid= $2 AND function_id < $3`;
  await userdb.query(query, newWorkflowID, workflowID, startStep);
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
