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
  request: string; // Serialized HTTPRequest
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
}

export interface notifications {
  destination_uuid: string;
  topic: string;
  message: string;
}

export interface workflow_events {
  workflow_uuid: string;
  key: string;
  value: string;
}

export interface operation_outputs {
  workflow_uuid: string;
  function_id: number;
  output: string;
  error: string;
  child_workflow_id: string;
  function_name?: string;
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

// This is the deserialized version of operation_outputs
export interface step_info {
  function_id: number;
  function_name: string;
  output: unknown;
  error: Error | null;
  child_workflow_id: string | null;
}
