import { WorkflowScheduleInternal } from '../system_database';
import { randomUUID } from 'crypto';

export interface WorkflowSchedule {
  scheduleId: string;
  scheduleName: string;
  workflowName: string;
  workflowClassName: string;
  schedule: string;
  status: string; // "ACTIVE" | "PAUSED"
  context: unknown; // deserialized
}

export function toWorkflowSchedule(internal: WorkflowScheduleInternal): WorkflowSchedule {
  let context: unknown;
  try {
    context = JSON.parse(internal.context);
  } catch {
    context = null;
  }

  return {
    scheduleId: internal.scheduleId,
    scheduleName: internal.scheduleName,
    workflowName: internal.workflowName,
    workflowClassName: internal.workflowClassName,
    schedule: internal.schedule,
    status: internal.status,
    context,
  };
}

export function createScheduleId(): string {
  return randomUUID();
}
