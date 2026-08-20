import {} from 'serialize-error';
import { PortableWorkflowError } from '../schemas/system_db_schema';

export function isDataValidationError(e: Error) {
  const dbosErrorCode = (e as DBOSError)?.dbosErrorCode;
  if (!dbosErrorCode) return false;
  if (dbosErrorCode === DataValidationError) {
    return true;
  }
  return false;
}

export class DBOSError extends Error {
  // TODO: define a better coding system.
  constructor(
    msg: string,
    readonly dbosErrorCode: number = 1,
  ) {
    super(msg);
  }
}

const InitializationError = 3;
export class DBOSInitializationError extends DBOSError {
  constructor(
    msg: string,
    readonly error?: Error,
  ) {
    super(msg, InitializationError);
  }
}

const ConflictingWFIDError = 5;
export class DBOSWorkflowConflictError extends DBOSError {
  constructor(workflowID: string) {
    super(`Conflicting WF ID ${workflowID}`, ConflictingWFIDError);
  }
}

const NotRegisteredError = 6;
export class DBOSNotRegisteredError extends DBOSError {
  constructor(name: string, fullmsg?: string) {
    const msg = fullmsg ?? `Operation (Name: ${name}) not registered`;
    super(msg, NotRegisteredError);
  }
}

const DataValidationError = 9;
export class DBOSDataValidationError extends DBOSError {
  constructor(msg: string) {
    super(msg, DataValidationError);
  }
}

const NotAuthorizedError = 12;
export class DBOSNotAuthorizedError extends DBOSError {
  constructor(
    msg: string,
    readonly status: number = 403,
  ) {
    super(msg, NotAuthorizedError);
  }
}

const ConfigKeyTypeError = 14;
export class DBOSConfigKeyTypeError extends DBOSError {
  constructor(configKey: string, expectedType: string, actualType: string) {
    super(`${configKey} should be of type ${expectedType}, but got ${actualType}`, ConfigKeyTypeError);
  }
}

const NonExistentWorkflowError = 16;
export class DBOSNonExistentWorkflowError extends DBOSError {
  constructor(msg: string) {
    super(msg, NonExistentWorkflowError);
  }
}

const FailLoadOperationsError = 17;
export class DBOSFailLoadOperationsError extends DBOSError {
  constructor(msg: string) {
    super(msg, FailLoadOperationsError);
  }
}

const MaxRecoveryAttemptsExceededError = 18;
export class DBOSMaxRecoveryAttemptsExceededError extends DBOSError {
  constructor(workflowID: string, maxRetries?: number) {
    super(
      `Workflow ${workflowID} has exceeded its maximum ${maxRetries === undefined ? 'number of' : `of ${maxRetries}`} execution or recovery attempts. Further attempts to execute or recover it will fail.`,
      MaxRecoveryAttemptsExceededError,
    );
  }
}

const ExecutorNotInitializedError = 20;
export class DBOSExecutorNotInitializedError extends DBOSError {
  constructor() {
    super('DBOS not initialized', ExecutorNotInitializedError);
  }
}

const InvalidWorkflowTransition = 21;
export class DBOSInvalidWorkflowTransitionError extends DBOSError {
  constructor(msg?: string) {
    super(msg ?? 'Invalid workflow state', InvalidWorkflowTransition);
  }
}

const ConflictingWorkflowError = 22;
export class DBOSConflictingWorkflowError extends DBOSError {
  constructor(workflowID: string, msg: string) {
    super(`Conflicting workflow invocation with the same ID (${workflowID}): ${msg}`, ConflictingWorkflowError);
  }
}

const MaximumRetriesError = 23;
export class DBOSMaxStepRetriesError extends DBOSError {
  readonly errors;
  constructor(stepName: string, maxRetries: number, errors: Error[]) {
    const formattedErrors = errors.map((error, index) => `Error ${index + 1}: ${error.message}`).join('. ');
    super(
      `Step ${stepName} has exceeded its maximum of ${maxRetries} retries. Previous errors: ${formattedErrors}`,
      MaximumRetriesError,
    );
    this.errors = errors;
  }
}

const WorkFlowCancelled = 24;
export class DBOSWorkflowCancelledError extends DBOSError {
  constructor(readonly workflowID: string) {
    super(`Workflow ${workflowID} has been cancelled`, WorkFlowCancelled);
  }
}

const ConflictingRegistrationError = 25;
export class DBOSConflictingRegistrationError extends DBOSError {
  constructor(msg: string) {
    super(msg, ConflictingRegistrationError);
  }
}

export const UnexpectedStep = 26;
/** Exception raised when a step has an unexpected recorded name, indicating a determinism problem. */
export class DBOSUnexpectedStepError extends DBOSError {
  constructor(
    readonly workflowID: string,
    readonly stepID: number,
    readonly expectedName: string,
    recordedName: string,
  ) {
    super(
      recordedName.startsWith('DBOS.patch')
        ? `During execution of workflow ${workflowID} step ${stepID}, function ${recordedName} was recorded when ${expectedName} was expected.\n
          Check that your patches are backward compatible, that you do not have older code trying to recover workflows with newer patches, and that your workflow is deterministic.`
        : `During execution of workflow ${workflowID} step ${stepID}, function ${recordedName} was recorded when ${expectedName} was expected. Check that your workflow is deterministic.`,
      UnexpectedStep,
    );
  }
}

const TargetWorkflowCancelled = 27;
export class DBOSAwaitedWorkflowCancelledError extends DBOSError {
  constructor(readonly workflowID: string) {
    super(`Awaited ${workflowID} was cancelled`, TargetWorkflowCancelled);
  }
}

export const QueueDedupIDDuplicated = 28;
/** Exception raised when workflow with same dedupid is queued*/
export class DBOSQueueDuplicatedError extends DBOSError {
  constructor(
    readonly workflowID: string,
    readonly queue: string,
    readonly deduplicationID: string,
  ) {
    super(
      `Workflow ${workflowID} was deduplicated due to an existing workflow in queue ${queue} with deduplication ID ${deduplicationID}.`,
      QueueDedupIDDuplicated,
    );
    // Portable error serialization records err.name, which is otherwise 'Error'; the debouncer matches on it after replay.
    this.name = 'DBOSQueueDuplicatedError';
  }
}

const InvalidQueuePriority = 29;
/** Exception raised queue priority is invalid */
export class DBOSInvalidQueuePriorityError extends DBOSError {
  constructor(
    readonly priority: number,
    readonly min: number,
    readonly max: number,
  ) {
    super(`Invalid priority ${priority}. Priority must be between ${min} and ${max}.`, InvalidQueuePriority);
  }
}

const AwaitedWorkflowExceededMaxRecoveryAttempts = 30;
export class DBOSAwaitedWorkflowExceededMaxRecoveryAttempts extends DBOSError {
  constructor(readonly workflowID: string) {
    super(`Awaited ${workflowID} exceeded its maximum recovery attempts`, AwaitedWorkflowExceededMaxRecoveryAttempts);
  }
}

const StepTimeout = 31;
/** Exception raised when a single attempt of a step exceeds its configured `timeoutMS` */
export class DBOSStepTimeoutError extends DBOSError {
  constructor(
    readonly stepName: string,
    readonly timeoutMS: number,
  ) {
    super(`Step ${stepName} timed out after ${timeoutMS}ms`, StepTimeout);
  }
}

const InvalidWorkflowInput = 32;
/** Exception raised when a workflow's arguments cannot be serialized. Blames the arguments alone. */
export class DBOSInvalidWorkflowInputError extends DBOSError {
  constructor(workflowName: string, cause: unknown) {
    super(
      `Could not serialize the arguments to workflow ${workflowName}: ${cause instanceof Error ? cause.message : String(cause)}`,
      InvalidWorkflowInput,
    );
  }
}

export const StreamTimeout = 33;
/** Exception raised when no value arrives on a stream within its timeout, or the stream ends before reaching a requested offset. */
export class DBOSStreamTimeoutError extends DBOSError {
  constructor(
    readonly workflowID: string,
    readonly key: string,
    readonly timeoutMS?: number,
  ) {
    // No timeout means the stream ended without reaching the value, so none ever will.
    super(
      `No value arrived on stream ${key} of workflow ${workflowID}${timeoutMS !== undefined ? ` within ${timeoutMS}ms` : ''}`,
      StreamTimeout,
    );
    // Error serialization records err.name, which is otherwise 'Error'; a replayed timeout is matched on it.
    this.name = 'DBOSStreamTimeoutError';
  }
}

/**
 * True if `e` is a stream-read timeout, including the portable-serialization
 * replay form, which carries only the original type name.
 *
 * A replayed error is revived as a plain `Error`, so `instanceof` does not hold; match on this.
 */
export function isStreamTimeoutError(e: unknown): boolean {
  if (e instanceof Error && getDBOSErrorCode(e) === StreamTimeout) {
    return true;
  }
  return e instanceof PortableWorkflowError && e.name === DBOSStreamTimeoutError.name;
}

export function getDBOSErrorCode(e: Error): number | undefined {
  if (e && typeof e === 'object' && 'dbosErrorCode' in e) {
    const code = (e as Record<string, unknown>).dbosErrorCode;
    return typeof code === 'number' ? code : undefined;
  }
  return undefined;
}
