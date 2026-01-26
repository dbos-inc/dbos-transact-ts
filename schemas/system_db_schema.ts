export interface workflow_status {
  workflow_uuid: string;
  status: string;
  name: string;
  class_name?: string;
  config_name?: string;
  authenticated_user: string;
  output: string;
  error: string;
  assumed_role: string;
  authenticated_roles: string; // Serialized list of roles.
  request: string; // Serialized event dispatch data (such as HTTPRequest)
  executor_id: string; // Set to "local" for local deployment, set to microVM ID for cloud deployment.
  application_version?: string;
  queue_name?: string;
  created_at: number;
  updated_at: number;
  application_id: string;
  recovery_attempts: number;
  workflow_timeout_ms: number | null;
  workflow_deadline_epoch_ms: number | null;
  inputs: string;
  started_at_epoch_ms?: number;
  deduplication_id?: string; // ID used to identify enqueued workflows for de-duplication.
  priority?: number; // Optional priority for the workflow.
  queue_partition_key?: string; // Partition key for partitioned queues.
  forked_from?: string;
  owner_xid?: string;
  serialization: string | null;
}

export interface notifications {
  destination_uuid: string;
  topic: string;
  message: string;
  serialization: string | null;
}

export interface workflow_events {
  workflow_uuid: string;
  key: string;
  value: string;
  serialization: string | null;
}

export interface operation_outputs {
  workflow_uuid: string;
  function_id: number;
  output: string;
  error: string;
  child_workflow_id: string;
  function_name?: string;
  started_at_epoch_ms?: number;
  completed_at_epoch_ms?: number;
}

export interface event_dispatch_kv {
  // Key fields
  service_name: string;
  workflow_fn_name: string;
  key: string;

  // Payload fields
  value?: string;
  update_time?: number; // Timestamp of record (for upsert)
  update_seq?: bigint; // Sequence number of record (for upsert)
}

export interface streams {
  workflow_uuid: string;
  key: string;
  value: string;
  offset: number;
  function_id: number;
}

export interface workflow_events_history {
  workflow_uuid: string;
  function_id: number;
  key: string;
  value: string;
  serialization: string | null;
}

// This is the deserialized version of operation_outputs
export interface step_info {
  function_id: number;
  function_name: string;
  output: unknown;
  error: Error | null;
  child_workflow_id: string | null;
  started_at_epoch_ms?: number;
  completed_at_epoch_ms?: number;
}

// This is system DB schema for portable inputs / outputs / messages / events / errors

// ---------- Canonical JSON value space ----------
// Note the absensce of "Date", etc.
// Canonical Date = RFC 3339 / ISO-8601 UTC string: YYYY-MM-DDTHH:mm:ss(.sss)Z
// This can be fixed with AJV (applied later)
export type JsonPrimitive = null | boolean | number | string;
export type JsonValue = JsonPrimitive | JsonObject | JsonArray;
export type JsonObject = { [k: string]: JsonValue };
export type JsonArray = JsonValue[];

// ---------- Workflow args + result ----------
export type JsonWorkflowArgs = {
  positionalArgs?: JsonArray;
  namedArgs?: JsonObject;
};
export type JsonWorkflowResult = JsonValue;
export interface JsonWorkflowErrorData {
  code: number | string;
  message: string; // Human-readable string
  data?: JsonValue; // structured details (retryable, origin, etc.)
}

// --------- Notification(Message) and WF event
export type JsonMessage = JsonValue;
export type JsonEvent = JsonValue;
