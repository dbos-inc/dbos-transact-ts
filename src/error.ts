export class OperonError extends Error {
  readonly msg: string;
  readonly code: number;

  constructor(code: number, msg: string) {
    super(msg);
    this.code = code;
    this.msg = msg;
  }
}