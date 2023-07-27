export class OperonError extends Error {
  readonly operonErrorCode: number;

  // TODO: define a better coding system.
  constructor(msg: string, public readonly code: number = 1) {
    super(msg);
    this.operonErrorCode = code;
  }
}