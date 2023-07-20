/* eslint-disable @typescript-eslint/no-explicit-any */
export type OperonCommunicator<T extends any[], R> = (ctxt: CommunicatorContext, ...args: T) => Promise<R | null>;

export interface CommunicatorParams {
  retriesAllowed?: boolean; // Should failures be retried? (default true)
  intervalSeconds?: number; // Seconds to wait before the first retry attempt (default 1).
  maxAttempts?: number; // Maximum number of retry attempts (default 3). If the error occurs more times than this, return null.
  backoffRate?: number; // The multiplier by which the retry interval increases after every retry attempt (default 2).
}

export class CommunicatorContext {

  readonly functionID: number;
  readonly retriesAllowed: boolean;
  readonly intervalSeconds: number;
  readonly maxAttempts: number;
  readonly backoffRate: number;


  // TODO: Validate the parameters.
  constructor(functionID: number, params: CommunicatorParams) {
    this.functionID = functionID;
    this.retriesAllowed = params.retriesAllowed ?? true;
    this.intervalSeconds = params.intervalSeconds ?? 1;
    this.maxAttempts = params.maxAttempts ?? 3;
    this.backoffRate = params.backoffRate ?? 2;
  }

}