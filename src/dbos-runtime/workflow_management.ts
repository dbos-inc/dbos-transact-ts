import { GetWorkflowsInput } from '..';
import { DBOSExecutor } from '../dbos-executor';
import { SystemDatabase, WorkflowStatusInternal } from '../system_database';
import { GetQueuedWorkflowsInput, WorkflowStatus } from '../workflow';
import { UserDatabase } from '../user_database';
import { DBOSJSON } from '../utils';
import { deserializeError } from 'serialize-error';
import { transaction_outputs } from '../../schemas/user_db_schema';

export interface StepInfo {
  readonly functionID: number;
  readonly name: string;
  readonly output: unknown;
  readonly error: Error | null;
  readonly childWorkflowID: string | null;
}

export async function $listWorkflows(
  sysdb: SystemDatabase,
  input: GetWorkflowsInput,
  getRequest: boolean = false,
): Promise<WorkflowStatus[]> {
  const workflows = await sysdb.listWorkflows(input);
  return workflows.map((wf) => $toWorkflowStatus(wf, getRequest));
}

export async function $listQueuedWorkflows(
  sysdb: SystemDatabase,
  input: GetQueuedWorkflowsInput,
  getRequest: boolean = false,
) {
  const workflows = await sysdb.listQueuedWorkflows(input);
  return workflows.map((wf) => $toWorkflowStatus(wf, getRequest));
}

export async function $getWorkflow(
  sysdb: SystemDatabase,
  workflowID: string,
  getRequest: boolean = false,
): Promise<WorkflowStatus | undefined> {
  const status = await sysdb.getWorkflowStatus(workflowID);
  return status ? $toWorkflowStatus(status, getRequest) : undefined;
}

export async function $listWorkflowSteps(
  sysdb: SystemDatabase,
  userdb: UserDatabase,
  workflowID: string,
): Promise<StepInfo[]> {
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

function $toWorkflowStatus(internal: WorkflowStatusInternal, getRequest: boolean = true): WorkflowStatus {
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

    request: getRequest ? internal.request : undefined,
    executorId: internal.executorId,
    applicationVersion: internal.applicationVersion,
    applicationID: internal.applicationID,
    recoveryAttempts: internal.recoveryAttempts,
    createdAt: internal.createdAt,
    updatedAt: internal.updatedAt,
  };
}
