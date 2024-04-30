export interface workflow_status {
  workflow_uuid: string;
  status: string;
  name: string;
  authenticated_user: string;
  output: string;
  error: string;
  assumed_role: string;
  authenticated_roles: string;  // Serialized list of roles.
  request: string;  // Serialized HTTPRequest
  executor_id: string;  // Set to "local" for local deployment, set to microVM ID for cloud deployment.
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
  wf_function: string;
  last_wf_sched_time: number; // Time that has certainly been kicked off; others may have but OAOO will cover that
}

export interface scheduled_wf_running {
  wf_function: string;
  scheduled_time: number;
  actual_time: number;
}