import { DBOSError } from './error';

/**
 * Configuration options for a `DBOS.step` function
 */
export interface StepConfig {
  /** If `true`, the step will be retried if it throws an exception (default false) */
  retriesAllowed?: boolean;
  /** seconds to wait before the first retry attempt (default 1). */
  intervalSeconds?: number;
  /** If `retriesAllowed` is true: maximum number of retry attempts (default 3). If errors occur more times than this, throw an exception. */
  maxAttempts?: number;
  /** If `retriesAllowed` is true: the multiplier by which the retry interval increases after every retry attempt (default 2) */
  backoffRate?: number;
  /** If `retriesAllowed` is true: called after a step throws to decide whether to retry that error. Defaults to retrying every error. */
  shouldRetry?: (error: unknown) => boolean | Promise<boolean>;
  /**
   * Maximum duration in milliseconds of a single attempt of this step.
   * An attempt exceeding this fails with `DBOSStepTimeoutError`; if `retriesAllowed` is true, it is retried like any other failure.
   * While an attempt runs, `DBOS.stepStatus.timeoutSignal` fires when the timeout expires so the step can cancel its underlying operation.
   * Note the attempt's code is not forcibly terminated: code that does not observe the signal keeps running in the background and its result is discarded.
   */
  timeoutMS?: number;
  /** If specified, override step function name */
  name?: string;
}

/** Validate a step configuration, throwing `DBOSError` if it is invalid. */
export function validateStepConfig(config: StepConfig, stepName: string): void {
  const timeoutMS = config.timeoutMS;
  if (timeoutMS !== undefined && (typeof timeoutMS !== 'number' || !Number.isFinite(timeoutMS) || timeoutMS <= 0)) {
    throw new DBOSError(
      `Invalid timeoutMS (${timeoutMS}) in configuration of step ${stepName}: must be a positive number`,
    );
  }
}
