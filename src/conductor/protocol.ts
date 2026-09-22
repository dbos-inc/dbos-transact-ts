import { inspect, type InspectOptions } from 'node:util';
import { serializeError } from 'serialize-error';
import type { StepInfo, WorkflowStatus } from '../workflow';

// Options for rendering a value into its human-readable Conductor string
// representation. Unlike inspect's defaults, these never truncate:
//   - `depth: null` keeps nested objects/arrays fully expanded instead of
//     collapsing deeper levels to `[Object]`/`[Array]` (issue #1313).
//   - lifting the array/string length caps avoids hiding large payloads.
// inspect (rather than JSON.stringify) is intentional: values may contain
// types that are not JSON-serializable — BigInt, Date, Map/Set, circular
// references — which must still render rather than break the view (issue #1167).
const CONDUCTOR_INSPECT_OPTIONS: InspectOptions = {
  depth: null,
  maxArrayLength: null,
  maxStringLength: null,
};

/**
 * Render a value into the human-readable string representation Conductor
 * displays for workflow/step inputs, outputs, and other context. Never throws
 * and never truncates.
 */
export function inspectForConductor(value: unknown): string {
  return inspect(value, CONDUCTOR_INSPECT_OPTIONS);
}

export enum MessageType {
  EXECUTOR_INFO = 'executor_info',
  RECOVERY = 'recovery',
  CANCEL = 'cancel',
  DELETE = 'delete',
  LIST_WORKFLOWS = 'list_workflows',
  LIST_QUEUED_WORKFLOWS = 'list_queued_workflows',
  RESUME = 'resume',
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
  LIST_APPLICATION_VERSIONS = 'list_application_versions',
  SET_LATEST_APPLICATION_VERSION = 'set_latest_application_version',
  GET_WORKFLOW_EVENTS = 'get_workflow_events',
  GET_WORKFLOW_NOTIFICATIONS = 'get_workflow_notifications',
  GET_WORKFLOW_STREAMS = 'get_workflow_streams',
  GET_WORKFLOW_AGGREGATES = 'get_workflow_aggregates',
  GET_STEP_AGGREGATES = 'get_step_aggregates',
  FORK_FROM_FAILURE = 'fork_from_failure',
  LIST_QUEUES = 'list_queues',
  GET_QUEUE = 'get_queue',
  REWIND_WORKFLOW = 'rewind_workflow',
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
  executor_metadata?: Record<string, unknown>;
  constructor(
    request_id: string,
    executor_id: string,
    application_version: string,
    hostname: string,
    language: string,
    dbos_version: string,
    error_message?: string,
    executor_metadata?: Record<string, unknown>,
  ) {
    super(MessageType.EXECUTOR_INFO, request_id, error_message);
    this.executor_id = executor_id;
    this.application_version = application_version;
    this.hostname = hostname;
    this.language = language;
    this.dbos_version = dbos_version;
    this.executor_metadata = executor_metadata;
  }
}

export interface RecoveryRequest extends BaseMessage {
  type: MessageType.RECOVERY;
  executor_ids: string[];
}

export class RecoveryResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.RECOVERY, request_id, error_message);
    this.success = success;
  }
}

export interface CancelRequest extends BaseMessage {
  type: MessageType.CANCEL;
  workflow_id: string;
  cancel_children: boolean;
  workflow_ids?: string[];
}

export class CancelResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.CANCEL, request_id, error_message);
    this.success = success;
  }
}

export interface DeleteRequest extends BaseMessage {
  type: MessageType.DELETE;
  workflow_id: string;
  delete_children: boolean;
  workflow_ids?: string[];
}

export class DeleteResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.DELETE, request_id, error_message);
    this.success = success;
  }
}

export interface ResumeRequest extends BaseMessage {
  type: MessageType.RESUME;
  workflow_id: string;
  workflow_ids?: string[];
  queue_name?: string;
}

export class ResumeResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.RESUME, request_id, error_message);
    this.success = success;
  }
}

export interface ListWorkflowsBody {
  workflow_uuids: string[];
  workflow_name?: string | string[];
  authenticated_user?: string | string[];
  start_time?: string;
  end_time?: string;
  completed_after?: string;
  completed_before?: string;
  dequeued_after?: string;
  dequeued_before?: string;
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
  was_forked_from?: boolean;
  has_parent?: boolean;
  attributes?: Record<string, unknown>;
  schedule_name?: string | string[];
  application_name?: string | string[];
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
  WasForkedFrom: boolean;
  ParentWorkflowID?: string;
  DelayUntilEpochMS?: string;
  CompletedAt?: string;
  Attributes?: string;
  ScheduleName?: string;
  ApplicationName?: string;

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
    this.Input = info.input ? inspectForConductor(info.input) : undefined;
    this.Output = info.output ? inspectForConductor(info.output) : undefined;
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
    this.WasForkedFrom = info.wasForkedFrom ?? false;
    this.ParentWorkflowID = info.parentWorkflowID;
    this.DelayUntilEpochMS = info.delayUntilEpochMS !== undefined ? String(info.delayUntilEpochMS) : undefined;
    this.CompletedAt = info.completedAt !== undefined ? String(info.completedAt) : undefined;
    // JSON rather than inspect() so the wire format stays parseable by Conductor.
    this.Attributes = info.attributes !== undefined ? JSON.stringify(info.attributes) : undefined;
    this.ScheduleName = info.scheduleName;
    this.ApplicationName = info.applicationName;
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
    this.output = info.output ? inspectForConductor(info.output) : undefined;
    this.error = info.error ? JSON.stringify(serializeError(info.error)) : undefined;
    this.child_workflow_id = info.childWorkflowID ?? undefined;
    this.started_at_epoch_ms = info.startedAtEpochMs !== undefined ? String(info.startedAtEpochMs) : undefined;
    this.completed_at_epoch_ms = info.completedAtEpochMs !== undefined ? String(info.completedAtEpochMs) : undefined;
  }
}

export interface ListWorkflowsRequest extends BaseMessage {
  type: MessageType.LIST_WORKFLOWS;
  body: ListWorkflowsBody;
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
  completed_after?: string;
  completed_before?: string;
  dequeued_after?: string;
  dequeued_before?: string;
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
  was_forked_from?: boolean;
  has_parent?: boolean;
  attributes?: Record<string, unknown>;
  schedule_name?: string | string[];
  application_name?: string | string[];
}

export interface ListQueuedWorkflowsRequest extends BaseMessage {
  type: MessageType.LIST_QUEUED_WORKFLOWS;
  body: ListQueuedWorkflowsBody;
}

export class ListQueuedWorkflowsResponse extends BaseResponse {
  output: WorkflowsOutput[];
  constructor(request_id: string, output: WorkflowsOutput[], error_message?: string) {
    super(MessageType.LIST_QUEUED_WORKFLOWS, request_id, error_message);
    this.output = output;
  }
}

export interface GetWorkflowRequest extends BaseMessage {
  type: MessageType.GET_WORKFLOW;
  workflow_id: string;
  load_input?: boolean;
  load_output?: boolean;
}

export class GetWorkflowResponse extends BaseResponse {
  output?: WorkflowsOutput;
  constructor(request_id: string, output?: WorkflowsOutput, error_message?: string) {
    super(MessageType.GET_WORKFLOW, request_id, error_message);
    this.output = output;
  }
}

export interface ExistPendingWorkflowsRequest extends BaseMessage {
  type: MessageType.EXIST_PENDING_WORKFLOWS;
  executor_id: string;
  application_version: string;
}

export class ExistPendingWorkflowsResponse extends BaseResponse {
  exist: boolean;
  constructor(request_id: string, exist: boolean, error_message?: string) {
    super(MessageType.EXIST_PENDING_WORKFLOWS, request_id, error_message);
    this.exist = exist;
  }
}

export interface ListStepsRequest extends BaseMessage {
  type: MessageType.LIST_STEPS;
  workflow_id: string;
  load_output?: boolean;
  limit?: number;
  offset?: number;
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
  queue_name?: string;
  queue_partition_key?: string;
}

export interface ForkWorkflowRequest extends BaseMessage {
  type: MessageType.FORK_WORKFLOW;
  body: ForkWorkflowBody;
}

export class ForkWorkflowResponse extends BaseResponse {
  new_workflow_id?: string;
  constructor(request_id: string, new_workflow_id?: string, error_message?: string) {
    super(MessageType.FORK_WORKFLOW, request_id, error_message);
    this.new_workflow_id = new_workflow_id;
  }
}

export interface RewindWorkflowBody {
  workflow_id: string;
  start_step?: number;
  application_version?: string;
  queue_name?: string;
  queue_partition_key?: string;
}

export interface RewindWorkflowRequest extends BaseMessage {
  type: MessageType.REWIND_WORKFLOW;
  body: RewindWorkflowBody;
}

export class RewindWorkflowResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.REWIND_WORKFLOW, request_id, error_message);
    this.success = success;
  }
}

export interface ForkFromFailureBody {
  workflow_ids: string[];
  application_version?: string;
  queue_name?: string;
  queue_partition_key?: string;
  from_last_failure?: boolean;
  from_last_step?: boolean;
  from_step?: number;
  from_step_name?: string;
}

export interface ForkFromFailureRequest extends BaseMessage {
  type: MessageType.FORK_FROM_FAILURE;
  body: ForkFromFailureBody;
}

export class ForkFromFailureResponse extends BaseResponse {
  forked_workflow_ids?: string[];
  constructor(request_id: string, forked_workflow_ids?: string[], error_message?: string) {
    super(MessageType.FORK_FROM_FAILURE, request_id, error_message);
    this.forked_workflow_ids = forked_workflow_ids;
  }
}

export interface RetentionBody {
  gc_cutoff_epoch_ms?: number;
  gc_rows_threshold?: number;
  // Absent from older Conductor versions, which take the default batch size.
  gc_batch_size?: number | null;
  timeout_cutoff_epoch_ms?: number;
}

export interface RetentionRequest extends BaseMessage {
  type: MessageType.RETENTION;
  body: RetentionBody;
}

export class RetentionResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.RETENTION, request_id, error_message);
    this.success = success;
  }
}

export interface GetMetricsRequest extends BaseMessage {
  type: MessageType.GET_METRICS;
  start_time: string;
  end_time: string;
  metric_class: string;
  // Optional so a Conductor predating the field still deserializes, as unscoped.
  application_name?: string[];
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

export interface ExportWorkflowRequest extends BaseMessage {
  type: MessageType.EXPORT_WORKFLOW;
  workflow_id: string;
  export_children: boolean;
}

export class ExportWorkflowResponse extends BaseResponse {
  serialized_workflow: string | null;
  constructor(request_id: string, serialized_workflow: string | null, error_message?: string) {
    super(MessageType.EXPORT_WORKFLOW, request_id, error_message);
    this.serialized_workflow = serialized_workflow;
  }
}

export interface ImportWorkflowRequest extends BaseMessage {
  type: MessageType.IMPORT_WORKFLOW;
  serialized_workflow: string;
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
  context?: string;
  last_fired_at: string | null;
  automatic_backfill: boolean;
  cron_timezone: string | null;
  queue_name: string | null;
  application_name: string | null;
}

export interface ListSchedulesBody {
  status?: string | string[];
  workflow_name?: string | string[];
  schedule_name_prefix?: string | string[];
  application_name?: string | string[];
  load_context?: boolean;
}

export interface ListSchedulesRequest extends BaseMessage {
  type: MessageType.LIST_SCHEDULES;
  body: ListSchedulesBody;
}

export class ListSchedulesResponse extends BaseResponse {
  output: ScheduleOutput[];
  constructor(request_id: string, output: ScheduleOutput[], error_message?: string) {
    super(MessageType.LIST_SCHEDULES, request_id, error_message);
    this.output = output;
  }
}

export interface GetScheduleRequest extends BaseMessage {
  type: MessageType.GET_SCHEDULE;
  schedule_name: string;
  load_context?: boolean;
}

export class GetScheduleResponse extends BaseResponse {
  output?: ScheduleOutput;
  constructor(request_id: string, output?: ScheduleOutput, error_message?: string) {
    super(MessageType.GET_SCHEDULE, request_id, error_message);
    this.output = output;
  }
}

export interface PauseScheduleRequest extends BaseMessage {
  type: MessageType.PAUSE_SCHEDULE;
  schedule_name: string;
}

export class PauseScheduleResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.PAUSE_SCHEDULE, request_id, error_message);
    this.success = success;
  }
}

export interface ResumeScheduleRequest extends BaseMessage {
  type: MessageType.RESUME_SCHEDULE;
  schedule_name: string;
}

export class ResumeScheduleResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.RESUME_SCHEDULE, request_id, error_message);
    this.success = success;
  }
}

export interface TriggerScheduleRequest extends BaseMessage {
  type: MessageType.TRIGGER_SCHEDULE;
  schedule_name: string;
}

export class TriggerScheduleResponse extends BaseResponse {
  workflow_id?: string;
  constructor(request_id: string, workflow_id?: string, error_message?: string) {
    super(MessageType.TRIGGER_SCHEDULE, request_id, error_message);
    this.workflow_id = workflow_id;
  }
}

export interface BackfillScheduleRequest extends BaseMessage {
  type: MessageType.BACKFILL_SCHEDULE;
  schedule_name: string;
  start: string;
  end: string;
}

export class BackfillScheduleResponse extends BaseResponse {
  workflow_ids: string[];
  constructor(request_id: string, workflow_ids: string[], error_message?: string) {
    super(MessageType.BACKFILL_SCHEDULE, request_id, error_message);
    this.workflow_ids = workflow_ids;
  }
}

// --- Application Versions protocol messages ---

export interface ApplicationVersionOutput {
  version_id: string;
  version_name: string;
  version_timestamp: number;
  created_at: number;
}

export class ListApplicationVersionsResponse extends BaseResponse {
  output: ApplicationVersionOutput[];
  constructor(request_id: string, output: ApplicationVersionOutput[], error_message?: string) {
    super(MessageType.LIST_APPLICATION_VERSIONS, request_id, error_message);
    this.output = output;
  }
}

export interface SetLatestApplicationVersionRequest extends BaseMessage {
  type: MessageType.SET_LATEST_APPLICATION_VERSION;
  version_name: string;
}

export class SetLatestApplicationVersionResponse extends BaseResponse {
  success: boolean;
  constructor(request_id: string, success: boolean, error_message?: string) {
    super(MessageType.SET_LATEST_APPLICATION_VERSION, request_id, error_message);
    this.success = success;
  }
}

// --- Workflow Communications Observability ---

export interface EventOutput {
  key: string;
  value: string;
}

export interface NotificationOutput {
  topic: string | null;
  message: string;
  created_at_epoch_ms: number;
  consumed: boolean;
}

export interface StreamEntryOutput {
  key: string;
  values: string[];
}

export interface GetWorkflowEventsRequest extends BaseMessage {
  type: MessageType.GET_WORKFLOW_EVENTS;
  workflow_id: string;
}

export class GetWorkflowEventsResponse extends BaseResponse {
  events?: EventOutput[];
  constructor(request_id: string, events?: EventOutput[], error_message?: string) {
    super(MessageType.GET_WORKFLOW_EVENTS, request_id, error_message);
    this.events = events;
  }
}

export interface GetWorkflowNotificationsRequest extends BaseMessage {
  type: MessageType.GET_WORKFLOW_NOTIFICATIONS;
  workflow_id: string;
}

export class GetWorkflowNotificationsResponse extends BaseResponse {
  notifications?: NotificationOutput[];
  constructor(request_id: string, notifications?: NotificationOutput[], error_message?: string) {
    super(MessageType.GET_WORKFLOW_NOTIFICATIONS, request_id, error_message);
    this.notifications = notifications;
  }
}

export interface GetWorkflowStreamsRequest extends BaseMessage {
  type: MessageType.GET_WORKFLOW_STREAMS;
  workflow_id: string;
}

export class GetWorkflowStreamsResponse extends BaseResponse {
  streams?: StreamEntryOutput[];
  constructor(request_id: string, streams?: StreamEntryOutput[], error_message?: string) {
    super(MessageType.GET_WORKFLOW_STREAMS, request_id, error_message);
    this.streams = streams;
  }
}

// --- Workflow Aggregates ---

export interface GetWorkflowAggregatesBody {
  group_by_status?: boolean;
  group_by_name?: boolean;
  group_by_queue_name?: boolean;
  group_by_executor_id?: boolean;
  group_by_application_version?: boolean;
  group_by_application_name?: boolean;
  select_count?: boolean;
  select_min_created_at?: boolean;
  select_max_queue_wait_ms?: boolean;
  select_max_total_latency_ms?: boolean;
  time_bucket_size_ms?: number;
  status?: string[];
  start_time?: string;
  end_time?: string;
  completed_after?: string;
  completed_before?: string;
  dequeued_after?: string;
  dequeued_before?: string;
  name?: string[];
  app_version?: string[];
  executor_id?: string[];
  queue_name?: string[];
  workflow_id_prefix?: string[];
  workflow_ids?: string[];
  user?: string[];
  forked_from?: string[];
  parent_workflow_id?: string[];
  schedule_name?: string[];
  application_name?: string[];
  was_forked_from?: boolean;
  has_parent?: boolean;
  attributes?: Record<string, unknown>;
}

export interface GetWorkflowAggregatesRequest extends BaseMessage {
  type: MessageType.GET_WORKFLOW_AGGREGATES;
  body: GetWorkflowAggregatesBody;
}

export interface WorkflowAggregateOutput {
  group: Record<string, string | null>;
  count?: number | null;
  min_created_at?: number | null;
  max_queue_wait_ms?: number | null;
  max_total_latency_ms?: number | null;
}

export class GetWorkflowAggregatesResponse extends BaseResponse {
  output: WorkflowAggregateOutput[];
  constructor(request_id: string, output: WorkflowAggregateOutput[], error_message?: string) {
    super(MessageType.GET_WORKFLOW_AGGREGATES, request_id, error_message);
    this.output = output;
  }
}

// --- Step Aggregates ---

export interface GetStepAggregatesBody {
  group_by_function_name?: boolean;
  group_by_status?: boolean;
  select_count?: boolean;
  select_max_duration_ms?: boolean;
  time_bucket_size_ms?: number;
  status?: string[];
  function_name?: string[];
  workflow_id_prefix?: string[];
  completed_after?: string;
  completed_before?: string;
  application_name?: string[];
}

export interface GetStepAggregatesRequest extends BaseMessage {
  type: MessageType.GET_STEP_AGGREGATES;
  body: GetStepAggregatesBody;
}

export interface StepAggregateOutput {
  group: Record<string, string | null>;
  count?: number | null;
  max_duration_ms?: number | null;
}

export class GetStepAggregatesResponse extends BaseResponse {
  output: StepAggregateOutput[];
  constructor(request_id: string, output: StepAggregateOutput[], error_message?: string) {
    super(MessageType.GET_STEP_AGGREGATES, request_id, error_message);
    this.output = output;
  }
}

// --- Queue messages ---

export interface QueueOutput {
  name: string;
  concurrency: number | null;
  worker_concurrency: number | null;
  rate_limit_max: number | null;
  rate_limit_period_sec: number | null;
  /** Always true: every queue dispatches in priority order. */
  priority_enabled: boolean;
  partition_queue: boolean;
  polling_interval_sec: number;
  application_name: string | null;
  partition_concurrency?: number | null;
  partition_worker_concurrency?: number | null;
  partition_rate_limit_max?: number | null;
  partition_rate_limit_period_sec?: number | null;
}

export interface ListQueuesBody {
  application_name?: string | string[];
}

export interface ListQueuesRequest extends BaseMessage {
  type: MessageType.LIST_QUEUES;
  // Optional: a Conductor that predates the filter sends no body.
  body?: ListQueuesBody;
}

export class ListQueuesResponse extends BaseResponse {
  output: QueueOutput[];
  constructor(request_id: string, output: QueueOutput[], error_message?: string) {
    super(MessageType.LIST_QUEUES, request_id, error_message);
    this.output = output;
  }
}

export interface GetQueueRequest extends BaseMessage {
  type: MessageType.GET_QUEUE;
  name: string;
}

export class GetQueueResponse extends BaseResponse {
  output: QueueOutput | null;
  constructor(request_id: string, output: QueueOutput | null, error_message?: string) {
    super(MessageType.GET_QUEUE, request_id, error_message);
    this.output = output;
  }
}
