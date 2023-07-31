export class OperonError extends Error {
  // TODO: define a better coding system.
  constructor(msg: string, readonly operonErrorCode: number = 1) {
    super(msg);
  }
}

const WorkflowPermissionDeniedError = 2;
export class OperonWorkflowPermissionDeniedError extends OperonError {
  constructor(runAs: string, workflowName: string) {
    const msg = `Subject ${runAs} does not have permission to run workflow ${workflowName})`;
    super(msg, WorkflowPermissionDeniedError);
  }
}
