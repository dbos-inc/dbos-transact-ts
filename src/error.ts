export class OperonError extends Error {
  readonly code: number;

  // TODO: define a better coding system.
  constructor(msg: string, code: number = 1) {
    super(msg);
    this.code = code;
  }
}

const PermissionDeniedError = 2;
export class OperonPermissionDeniedError extends OperonError {
  constructor(runAs: string, workflowName: string | undefined, workflowId: string | undefined) {
    const msg =
        `User ${runAs} does not have permission to run workflow ${workflowName} (id: ${workflowId})`;
    super(msg, PermissionDeniedError);
  }
}
