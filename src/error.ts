export class OperonError extends Error {
  readonly code: number;

  // TODO: define a better coding system.
  constructor(msg: string, code: number = 1) {
    super(msg);
    this.code = code;
  }
}