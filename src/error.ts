import { WorkflowConfig } from './workflow';

export class OperonError extends Error {
  readonly code: number;

  // TODO: define a better coding system.
  constructor(msg: string, code: number = 1) {
    super(msg);
    this.code = code;
  }
}

const WorkflowPermissionDeniedError = 2;
export class OperonWorkflowPermissionDeniedError extends OperonError {
  constructor(runAs: string, workflowConfig: WorkflowConfig) {
    const msg =
      `Subject ${runAs} does not have permission to`
      + `run workflow ${workflowConfig.name} (id: ${workflowConfig.id})`;
    super(msg, WorkflowPermissionDeniedError);
  }
}
