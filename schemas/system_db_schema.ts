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
  authenticated_roles: string;  // Serialized list of roles.
  request: string;  // Serialized HTTPRequest
  executor_id: string;  // Set to "local" for local deployment, set to microVM ID for cloud deployment.
  application_version: string;
  queue_name?: string;
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
}

export interface workflow_inputs {
  workflow_uuid: string;
  inputs: string;
}

export interface scheduler_state {
  workflow_fn_name: string;
  last_run_time: number; // Time that has certainly been kicked off; others may have but OAOO will cover that
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

export interface workflow_queue {
  workflow_uuid: string;
  queue_name: string;
  created_at_epoch_ms: number; // This time is provided by the database
  started_at_epoch_ms?: number; // This time is provided by the client
  completed_at_epoch_ms?: number; // This time is provided by the client
}
