import { inspect } from 'node:util';
import { serializeError } from 'serialize-error';
import type { StepInfo, WorkflowStatus } from '../workflow';

export enum MessageType {
  EXECUTOR_INFO = 'executor_info',
  RECOVERY = 'recovery',
  CANCEL = 'cancel',
  DELETE = 'delete',
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
  EXPORT_WORKFLOW = 'export_workflow',
  IMPORT_WORKFLOW = 'import_workflow',
  ALERT = 'alert',
  LIST_SCHEDULES = 'list_schedules',
  GET_SCHEDULE = 'get_schedule',
  PAUSE_SCHEDULE = 'pause_schedule',
  RESUME_SCHEDULE = 'resume_schedule',
  BACKFILL_SCHEDULE = 'backfill_schedule',
  TRIGGER_SCHEDULE = 'trigger_schedule',
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

export class DeleteRequest implements BaseMessage {
  type = MessageType.DELETE;
  request_id: string;
  workflow_id: string;
  delete_children: boolean;
  constructor(request_id: string, workflow_id: string, delete_children: boolean = false) {
    this.request_id = request_id;
    this.workflow_id = workflow_id;
    this.delete_children = delete_children;
  }
}

export class DeleteResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.DELETE, request_id, error_message);
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
  workflow_name?: string | string[];
  authenticated_user?: string | string[];
  start_time?: string;
  end_time?: string;
  status?: string | string[];
  application_version?: string | string[];
  forked_from?: string | string[];
  parent_workflow_id?: string | string[];
  queue_name?: string | string[];
  limit?: number;
  offset?: number;
  sort_desc: boolean;
  workflow_id_prefix?: string | string[];
  load_input?: boolean; // Load the input of the workflow (default false)
  load_output?: boolean; // Load the output of the workflow (default false)
  executor_id?: string | string[];
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
  DequeuedAt?: string;
  ForkedFrom?: string;
  ParentWorkflowID?: string;

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
    this.Input = info.input ? inspect(info.input) : undefined;
    this.Output = info.output ? inspect(info.output) : undefined;
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
    this.DequeuedAt = info.dequeuedAt !== undefined ? String(info.dequeuedAt) : undefined;
    this.ForkedFrom = info.forkedFrom;
    this.ParentWorkflowID = info.parentWorkflowID;
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
    this.output = info.output ? inspect(info.output) : undefined;
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
  workflow_name?: string | string[];
  authenticated_user?: string | string[];
  start_time?: string;
  end_time?: string;
  status?: string | string[];
  application_version?: string | string[];
  forked_from?: string | string[];
  parent_workflow_id?: string | string[];
  queue_name?: string | string[];
  limit?: number;
  offset?: number;
  sort_desc: boolean;
  workflow_id_prefix?: string | string[];
  load_input?: boolean; // Load the input of the workflow (default false)
  load_output?: boolean; // Load the output of the workflow (default false)
  executor_id?: string | string[];
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

export class ExportWorkflowRequest implements BaseMessage {
  type = MessageType.EXPORT_WORKFLOW;
  request_id: string;
  workflow_id: string;
  export_children: boolean;
  constructor(request_id: string, workflow_id: string, export_children: boolean = false) {
    this.request_id = request_id;
    this.workflow_id = workflow_id;
    this.export_children = export_children;
  }
}

export class ExportWorkflowResponse extends BaseResponse {
  serialized_workflow: string | null;
  constructor(request_id: string, serialized_workflow: string | null, error_message?: string) {
    super(MessageType.EXPORT_WORKFLOW, request_id, error_message);
    this.serialized_workflow = serialized_workflow;
  }
}

export class ImportWorkflowRequest implements BaseMessage {
  type = MessageType.IMPORT_WORKFLOW;
  request_id: string;
  serialized_workflow: string;
  constructor(request_id: string, serialized_workflow: string) {
    this.request_id = request_id;
    this.serialized_workflow = serialized_workflow;
  }
}

export class ImportWorkflowResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.IMPORT_WORKFLOW, request_id, error_message);
    this.success = success;
  }
}

export interface AlertRequest extends BaseMessage {
  type: MessageType.ALERT;
  name: string;
  message: string;
  metadata: Record<string, string>;
}

export class AlertResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.ALERT, request_id, error_message);
    this.success = success;
  }
}

// --- Schedule protocol messages ---

export interface ScheduleOutput {
  schedule_id: string;
  schedule_name: string;
  workflow_name: string;
  workflow_class_name?: string;
  schedule: string;
  status: string;
  context: string;
}

export interface ListSchedulesBody {
  status?: string | string[];
  workflow_name?: string | string[];
  schedule_name_prefix?: string | string[];
}

export class ListSchedulesRequest implements BaseMessage {
  type = MessageType.LIST_SCHEDULES;
  request_id: string;
  body: ListSchedulesBody;
  constructor(request_id: string, body: ListSchedulesBody) {
    this.request_id = request_id;
    this.body = body;
  }
}

export class ListSchedulesResponse extends BaseResponse {
  output: ScheduleOutput[];
  constructor(request_id: string, output: ScheduleOutput[], error_message?: string) {
    super(MessageType.LIST_SCHEDULES, request_id, error_message);
    this.output = output;
  }
}

export class GetScheduleRequest implements BaseMessage {
  type = MessageType.GET_SCHEDULE;
  request_id: string;
  schedule_name: string;
  constructor(request_id: string, schedule_name: string) {
    this.request_id = request_id;
    this.schedule_name = schedule_name;
  }
}

export class GetScheduleResponse extends BaseResponse {
  output?: ScheduleOutput;
  constructor(request_id: string, output?: ScheduleOutput, error_message?: string) {
    super(MessageType.GET_SCHEDULE, request_id, error_message);
    this.output = output;
  }
}

export class PauseScheduleRequest implements BaseMessage {
  type = MessageType.PAUSE_SCHEDULE;
  request_id: string;
  schedule_name: string;
  constructor(request_id: string, schedule_name: string) {
    this.request_id = request_id;
    this.schedule_name = schedule_name;
  }
}

export class PauseScheduleResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.PAUSE_SCHEDULE, request_id, error_message);
    this.success = success;
  }
}

export class ResumeScheduleRequest implements BaseMessage {
  type = MessageType.RESUME_SCHEDULE;
  request_id: string;
  schedule_name: string;
  constructor(request_id: string, schedule_name: string) {
    this.request_id = request_id;
    this.schedule_name = schedule_name;
  }
}

export class ResumeScheduleResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.RESUME_SCHEDULE, request_id, error_message);
    this.success = success;
  }
}

export class TriggerScheduleRequest implements BaseMessage {
  type = MessageType.TRIGGER_SCHEDULE;
  request_id: string;
  schedule_name: string;
  constructor(request_id: string, schedule_name: string) {
    this.request_id = request_id;
    this.schedule_name = schedule_name;
  }
}

export class TriggerScheduleResponse extends BaseResponse {
  workflow_id?: string;
  constructor(request_id: string, workflow_id?: string, error_message?: string) {
    super(MessageType.TRIGGER_SCHEDULE, request_id, error_message);
    this.workflow_id = workflow_id;
  }
}

export class BackfillScheduleRequest implements BaseMessage {
  type = MessageType.BACKFILL_SCHEDULE;
  request_id: string;
  schedule_name: string;
  start: string;
  end: string;
  constructor(request_id: string, schedule_name: string, start: string, end: string) {
    this.request_id = request_id;
    this.schedule_name = schedule_name;
    this.start = start;
    this.end = end;
  }
}

export class BackfillScheduleResponse extends BaseResponse {
  workflow_ids: string[];
  constructor(request_id: string, workflow_ids: string[], error_message?: string) {
    super(MessageType.BACKFILL_SCHEDULE, request_id, error_message);
    this.workflow_ids = workflow_ids;
  }
}
