import { serializeError } from 'serialize-error';
import type { StepInfo, WorkflowStatus } from '../workflow';

export enum MessageType {
  EXECUTOR_INFO = 'executor_info',
  RECOVERY = 'recovery',
  CANCEL = 'cancel',
  LIST_WORKFLOWS = 'list_workflows',
  LIST_QUEUED_WORKFLOWS = 'list_queued_workflows',
  RESUME = 'resume',
  RESTART = 'restart',
  GET_WORKFLOW = 'get_workflow',
  EXIST_PENDING_WORKFLOWS = 'exist_pending_workflows',
  LIST_STEPS = 'list_steps',
  FORK_WORKFLOW = 'fork_workflow',
  RETENTION = 'retention',
  GET_METRICS = 'get_metrics',
}

export interface BaseMessage {
  type: MessageType;
  request_id: string;
}

export class BaseResponse implements BaseMessage {
  type: MessageType;
  request_id: string;
  error_message?: string;
  constructor(type: MessageType, request_id: string, error_message?: string) {
    this.type = type;
    this.request_id = request_id;
    this.error_message = error_message;
  }
}

export class ExecutorInfoResponse extends BaseResponse {
  executor_id: string;
  application_version: string;
  hostname: string;
  language: string;
  dbos_version: string;
  constructor(
    request_id: string,
    executor_id: string,
    application_version: string,
    hostname: string,
    language: string,
    dbos_version: string,
    error_message?: string,
  ) {
    super(MessageType.EXECUTOR_INFO, request_id, error_message);
    this.executor_id = executor_id;
    this.application_version = application_version;
    this.hostname = hostname;
    this.language = language;
    this.dbos_version = dbos_version;
  }
}

export class RecoveryRequest implements BaseMessage {
  type = MessageType.RECOVERY;
  request_id: string;
  executor_ids: string[];
  constructor(request_id: string, executor_ids: string[]) {
    this.request_id = request_id;
    this.executor_ids = executor_ids;
  }
}

export class RecoveryResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.RECOVERY, request_id, error_message);
    this.success = success;
  }
}

export class CancelRequest implements BaseMessage {
  type = MessageType.CANCEL;
  request_id: string;
  workflow_id: string;
  constructor(request_id: string, workflow_id: string) {
    this.request_id = request_id;
    this.workflow_id = workflow_id;
  }
}

export class CancelResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.CANCEL, request_id, error_message);
    this.success = success;
  }
}

export class ResumeRequest implements BaseMessage {
  type = MessageType.RESUME;
  request_id: string;
  workflow_id: string;
  constructor(request_id: string, workflow_id: string) {
    this.request_id = request_id;
    this.workflow_id = workflow_id;
  }
}

export class ResumeResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.RESUME, request_id, error_message);
    this.success = success;
  }
}

export class RestartRequest implements BaseMessage {
  type = MessageType.RESTART;
  request_id: string;
  workflow_id: string;
  constructor(request_id: string, workflow_id: string) {
    this.request_id = request_id;
    this.workflow_id = workflow_id;
  }
}

export class RestartResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.RESTART, request_id, error_message);
    this.success = success;
  }
}

export interface ListWorkflowsBody {
  workflow_uuids: string[];
  workflow_name?: string;
  authenticated_user?: string;
  start_time?: string;
  end_time?: string;
  status?: string;
  application_version?: string;
  forked_from?: string;
  queue_name?: string;
  limit?: number;
  offset?: number;
  sort_desc: boolean;
  workflow_id_prefix?: string;
  load_input?: boolean; // Load the input of the workflow (default false)
  load_output?: boolean; // Load the output of the workflow (default false)
  executor_id?: string;
  queues_only?: boolean;
}

export class WorkflowsOutput {
  WorkflowUUID: string;
  Status?: string;
  WorkflowName?: string;
  WorkflowClassName?: string;
  WorkflowConfigName?: string;
  AuthenticatedUser?: string;
  AssumedRole?: string;
  AuthenticatedRoles?: string;
  Input?: string;
  Output?: string;
  Error?: string;
  CreatedAt?: string;
  UpdatedAt?: string;
  QueueName?: string;
  ApplicationVersion?: string;
  ExecutorID?: string;
  WorkflowTimeoutMS?: string;
  WorkflowDeadlineEpochMS?: string;
  DeduplicationID?: string;
  Priority?: string;
  QueuePartitionKey?: string;
  ForkedFrom?: string;

  constructor(info: WorkflowStatus) {
    // Mark empty fields as undefined
    this.WorkflowUUID = info.workflowID;
    this.Status = info.status;
    this.WorkflowName = info.workflowName;
    this.WorkflowClassName = info.workflowClassName ? info.workflowClassName : undefined;
    this.WorkflowConfigName = info.workflowConfigName ? info.workflowConfigName : undefined;
    this.AuthenticatedUser = info.authenticatedUser ? info.authenticatedUser : undefined;
    this.AssumedRole = info.assumedRole ? info.assumedRole : undefined;
    this.AuthenticatedRoles =
      (info.authenticatedRoles ?? []).length > 0 ? JSON.stringify(info.authenticatedRoles) : undefined;
    this.Input = info.input ? JSON.stringify(info.input) : undefined;
    this.Output = info.output ? JSON.stringify(info.output) : undefined;
    this.Error = info.error ? JSON.stringify(serializeError(info.error)) : undefined;
    this.CreatedAt = info.createdAt ? String(info.createdAt) : undefined;
    this.UpdatedAt = info.updatedAt ? String(info.updatedAt) : undefined;
    this.QueueName = info.queueName ? info.queueName : undefined;
    this.ApplicationVersion = info.applicationVersion;
    this.ExecutorID = info.executorId;
    this.WorkflowTimeoutMS = info.timeoutMS !== undefined ? String(info.timeoutMS) : undefined;
    this.WorkflowDeadlineEpochMS = info.deadlineEpochMS !== undefined ? String(info.deadlineEpochMS) : undefined;
    this.DeduplicationID = info.deduplicationID;
    this.Priority = String(info.priority);
    this.QueuePartitionKey = info.queuePartitionKey;
    this.ForkedFrom = info.forkedFrom;
  }
}

export class WorkflowSteps {
  function_id: number;
  function_name: string;
  output?: string;
  error?: string;
  child_workflow_id?: string;
  started_at_epoch_ms?: string;
  completed_at_epoch_ms?: string;

  constructor(info: StepInfo) {
    this.function_id = info.functionID;
    this.function_name = info.name;
    this.output = info.output ? JSON.stringify(info.output) : undefined;
    this.error = info.error ? JSON.stringify(serializeError(info.error)) : undefined;
    this.child_workflow_id = info.childWorkflowID ?? undefined;
    this.started_at_epoch_ms = info.startedAtEpochMs !== undefined ? String(info.startedAtEpochMs) : undefined;
    this.completed_at_epoch_ms = info.completedAtEpochMs !== undefined ? String(info.completedAtEpochMs) : undefined;
  }
}

export class ListWorkflowsRequest implements BaseMessage {
  type = MessageType.LIST_WORKFLOWS;
  request_id: string;
  body: ListWorkflowsBody;
  constructor(request_id: string, body: ListWorkflowsBody) {
    this.request_id = request_id;
    this.body = body;
  }
}

export class ListWorkflowsResponse extends BaseResponse {
  output: WorkflowsOutput[];
  constructor(request_id: string, output: WorkflowsOutput[], error_message?: string) {
    super(MessageType.LIST_WORKFLOWS, request_id, error_message);
    this.output = output;
  }
}

export interface ListQueuedWorkflowsBody {
  workflow_uuids?: string[];
  workflow_name?: string;
  authenticated_user?: string;
  start_time?: string;
  end_time?: string;
  status?: string;
  application_version?: string;
  forked_from?: string;
  queue_name?: string;
  limit?: number;
  offset?: number;
  sort_desc: boolean;
  workflow_id_prefix?: string;
  load_input?: boolean; // Load the input of the workflow (default false)
  load_output?: boolean; // Load the output of the workflow (default false)
  executor_id?: string;
}

export class ListQueuedWorkflowsRequest implements BaseMessage {
  type = MessageType.LIST_QUEUED_WORKFLOWS;
  request_id: string;
  body: ListQueuedWorkflowsBody;
  constructor(request_id: string, body: ListQueuedWorkflowsBody) {
    this.request_id = request_id;
    this.body = body;
  }
}

export class ListQueuedWorkflowsResponse extends BaseResponse {
  output: WorkflowsOutput[];
  constructor(request_id: string, output: WorkflowsOutput[], error_message?: string) {
    super(MessageType.LIST_QUEUED_WORKFLOWS, request_id, error_message);
    this.output = output;
  }
}

export class GetWorkflowRequest implements BaseMessage {
  type = MessageType.GET_WORKFLOW;
  request_id: string;
  workflow_id: string;
  constructor(request_id: string, workflow_id: string) {
    this.request_id = request_id;
    this.workflow_id = workflow_id;
  }
}

export class GetWorkflowResponse extends BaseResponse {
  output?: WorkflowsOutput;
  constructor(request_id: string, output?: WorkflowsOutput, error_message?: string) {
    super(MessageType.GET_WORKFLOW, request_id, error_message);
    this.output = output;
  }
}

export class ExistPendingWorkflowsRequest implements BaseMessage {
  type = MessageType.EXIST_PENDING_WORKFLOWS;
  request_id: string;
  executor_id: string;
  application_version: string;
  constructor(request_id: string, executor_id: string, application_version: string) {
    this.request_id = request_id;
    this.executor_id = executor_id;
    this.application_version = application_version;
  }
}

export class ExistPendingWorkflowsResponse extends BaseResponse {
  exist: boolean;
  constructor(request_id: string, exist: boolean, error_message?: string) {
    super(MessageType.EXIST_PENDING_WORKFLOWS, request_id, error_message);
    this.exist = exist;
  }
}

export class ListStepsRequest implements BaseMessage {
  type = MessageType.LIST_STEPS;
  request_id: string;
  workflow_id: string;
  constructor(request_id: string, workflow_id: string) {
    this.request_id = request_id;
    this.workflow_id = workflow_id;
  }
}

export class ListStepsResponse extends BaseResponse {
  output?: WorkflowSteps[];
  constructor(request_id: string, output?: WorkflowSteps[], error_message?: string) {
    super(MessageType.LIST_STEPS, request_id, error_message);
    this.output = output;
  }
}

export interface ForkWorkflowBody {
  workflow_id: string;
  start_step: number;
  application_version?: string;
  new_workflow_id?: string;
}

export class ForkWorkflowRequest implements BaseMessage {
  type = MessageType.FORK_WORKFLOW;
  request_id: string;
  body: ForkWorkflowBody;
  constructor(request_id: string, body: ForkWorkflowBody) {
    this.request_id = request_id;
    this.body = body;
  }
}

export class ForkWorkflowResponse extends BaseResponse {
  new_workflow_id?: string;
  constructor(request_id: string, new_workflow_id?: string, error_message?: string) {
    super(MessageType.FORK_WORKFLOW, request_id, error_message);
    this.new_workflow_id = new_workflow_id;
  }
}

export interface RetentionBody {
  gc_cutoff_epoch_ms?: number;
  gc_rows_threshold?: number;
  timeout_cutoff_epoch_ms?: number;
}

export class RetentionRequest implements BaseMessage {
  type = MessageType.RETENTION;
  request_id: string;
  body: RetentionBody;
  constructor(request_id: string, body: RetentionBody) {
    this.request_id = request_id;
    this.body = body;
  }
}

export class RetentionResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.RETENTION, request_id, error_message);
    this.success = success;
  }
}

export class GetMetricsRequest implements BaseMessage {
  type = MessageType.GET_METRICS;
  request_id: string;
  start_time: string;
  end_time: string;
  metric_class: string;
  constructor(request_id: string, start_time: string, end_time: string, metric_class: string) {
    this.request_id = request_id;
    this.start_time = start_time;
    this.end_time = end_time;
    this.metric_class = metric_class;
  }
}

export class MetricDataOutput {
  metric_type: string;
  metric_name: string;
  value: number;
  constructor(metric_type: string, metric_name: string, value: number) {
    this.metric_type = metric_type;
    this.metric_name = metric_name;
    this.value = value;
  }
}

export class GetMetricsResponse extends BaseResponse {
  metrics: MetricDataOutput[];
  constructor(request_id: string, metrics: MetricDataOutput[], error_message?: string) {
    super(MessageType.GET_METRICS, request_id, error_message);
    this.metrics = metrics;
  }
}
