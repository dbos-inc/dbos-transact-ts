import { Span } from '@opentelemetry/sdk-trace-base';
import { GlobalLogger as Logger } from './telemetry/logs';
import { WorkflowContextImpl } from './workflow';
import { DBOSContext, DBOSContextImpl } from './context';

/** @deprecated */
export type StepFunction<T extends unknown[], R> = (ctxt: StepContext, ...args: T) => Promise<R>;

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

/**
 * @deprecated This class is no longer necessary
 * To update to Transact 2.0+
 *   Remove `StepContext` from function parameter lists
 *   Use `DBOS.` to access DBOS context within affected functions
 *   Adjust callers to call the function directly
 */
export interface StepContext extends DBOSContext {
  // These fields reflect the communictor's configuration.
  readonly retriesAllowed: boolean;
  readonly maxAttempts: number;
}

export class StepContextImpl extends DBOSContextImpl implements StepContext {
  readonly functionID: number;
  readonly retriesAllowed: boolean;
  readonly intervalSeconds: number;
  readonly maxAttempts: number;
  readonly backoffRate: number;

  // TODO: Validate the parameters.
  constructor(
    workflowContext: WorkflowContextImpl,
    functionID: number,
    span: Span,
    logger: Logger,
    params: StepConfig,
    commName: string,
  ) {
    super(commName, span, logger, workflowContext);
    this.functionID = functionID;
    this.retriesAllowed = params.retriesAllowed ?? false;
    this.intervalSeconds = params.intervalSeconds ?? 1;
    this.maxAttempts = params.maxAttempts ?? 3;
    this.backoffRate = params.backoffRate ?? 2;
    this.applicationConfig = workflowContext.applicationConfig;
  }
}
