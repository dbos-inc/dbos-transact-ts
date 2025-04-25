import { serializeError } from 'serialize-error';
import { DBOSJSON } from '../utils';
import { WorkflowStatus } from '../workflow';
import { StepInfo } from '../dbos-runtime/workflow_management';

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
  constructor(
    request_id: string,
    executor_id: string,
    application_version: string,
    hostname: string,
    error_message?: string,
  ) {
    super(MessageType.EXECUTOR_INFO, request_id, error_message);
    this.executor_id = executor_id;
    this.application_version = application_version;
    this.hostname = hostname;
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
  limit?: number;
  offset?: number;
  sort_desc: boolean;
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
  Request?: string;
  Error?: string;
  CreatedAt?: string;
  UpdatedAt?: string;
  QueueName?: string;
  ApplicationVersion?: string;
  ExecutorID?: string;

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
      (info.authenticatedRoles ?? []).length > 0 ? DBOSJSON.stringify(info.authenticatedRoles) : undefined;
    this.Input = info.input ? DBOSJSON.stringify(info.input) : undefined;
    this.Output = info.output ? DBOSJSON.stringify(info.output) : undefined;
    this.Request = info.request ? DBOSJSON.stringify(info.request) : undefined;
    this.Error = info.error ? DBOSJSON.stringify(serializeError(info.error)) : undefined;
    this.CreatedAt = info.createdAt ? String(info.createdAt) : undefined;
    this.UpdatedAt = info.updatedAt ? String(info.updatedAt) : undefined;
    this.QueueName = info.queueName ? info.queueName : undefined;
    this.ApplicationVersion = info.applicationVersion;
    this.ExecutorID = info.executorId;
  }
}

export class WorkflowSteps {
  function_id: number;
  function_name: string;
  output?: string;
  error?: string;
  child_workflow_id?: string;

  constructor(info: StepInfo) {
    this.function_id = info.functionID;
    this.function_name = info.name;
    this.output = info.output ? DBOSJSON.stringify(info.output) : undefined;
    this.error = info.error ? DBOSJSON.stringify(serializeError(info.error)) : undefined;
    this.child_workflow_id = info.childWorkflowID ?? undefined;
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
  workflow_name?: string;
  start_time?: string;
  end_time?: string;
  status?: string;
  queue_name?: string;
  limit?: number;
  offset?: number;
  sort_desc: boolean;
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
