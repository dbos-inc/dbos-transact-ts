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
}
