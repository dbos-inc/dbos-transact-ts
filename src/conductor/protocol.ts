import { serializeError } from 'serialize-error';
import { WorkflowInformation } from '../dbos-runtime/workflow_management';
import { DBOSJSON } from '../utils';

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
}

export interface BaseMessage {
  type: MessageType;
  request_id: string;
}

export class ExecutorInfoResponse implements BaseMessage {
  type = MessageType.EXECUTOR_INFO;
  request_id: string;
  executor_id: string;
  application_version: string;
  constructor(request_id: string, executor_id: string, application_version: string) {
    this.request_id = request_id;
    this.executor_id = executor_id;
    this.application_version = application_version;
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

export class RecoveryResponse implements BaseMessage {
  type = MessageType.RECOVERY;
  request_id: string;
  success: boolean;
  constructor(request_id: string, success: boolean) {
    this.request_id = request_id;
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

  constructor(info: WorkflowInformation) {
    this.WorkflowUUID = info.workflowUUID;
    this.Status = info.status;
    this.WorkflowName = info.workflowName;
    this.WorkflowClassName = info.workflowClassName;
    this.WorkflowConfigName = info.workflowConfigName;
    this.AuthenticatedUser = info.authenticatedUser;
    this.AssumedRole = info.assumedRole;
    this.AuthenticatedRoles = DBOSJSON.stringify(info.authenticatedRoles);
    this.Input = info.input ? DBOSJSON.stringify(info.input) : undefined;
    this.Output = info.output ? DBOSJSON.stringify(info.output) : undefined;
    this.Request = info.request ? DBOSJSON.stringify(info.request) : undefined;
    this.Error = info.error ? DBOSJSON.stringify(serializeError(info.error)) : undefined;
    // TODO: add missing fields
    // this.CreatedAt = info.created_at ? String(info.created_at) : undefined;
    // this.UpdatedAt = info.updated_at ? String(info.updated_at) : undefined;
    this.QueueName = info.queueName;
    // this.ApplicationVersion = info.app_version;
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

export class ListWorkflowsResponse implements BaseMessage {
  type = MessageType.LIST_WORKFLOWS;
  request_id: string;
  output: WorkflowsOutput[];
  constructor(request_id: string, output: WorkflowsOutput[]) {
    this.request_id = request_id;
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

export class ListQueuedWorkflowsResponse implements BaseMessage {
  type = MessageType.LIST_QUEUED_WORKFLOWS;
  request_id: string;
  output: WorkflowsOutput[];
  constructor(request_id: string, output: WorkflowsOutput[]) {
    this.request_id = request_id;
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

export class GetWorkflowResponse implements BaseMessage {
  type = MessageType.GET_WORKFLOW;
  request_id: string;
  output?: WorkflowsOutput;
  constructor(request_id: string, output?: WorkflowsOutput) {
    this.request_id = request_id;
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

export class ExistPendingWorkflowsResponse implements BaseMessage {
  type = MessageType.EXIST_PENDING_WORKFLOWS;
  request_id: string;
  exist: boolean;
  constructor(request_id: string, exist: boolean) {
    this.request_id = request_id;
    this.exist = exist;
  }
}
