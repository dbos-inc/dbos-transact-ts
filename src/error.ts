import {} from 'serialize-error';

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

const DebuggerError = 15;
export class DBOSDebuggerError extends DBOSError {
  constructor(msg: string) {
    super('DEBUGGER: ' + msg, DebuggerError);
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
  constructor(workflowID: string, maxRetries: number) {
    super(
      `Workflow ${workflowID} has exceeded its maximum of ${maxRetries} execution or recovery attempts. Further attempts to execute or recover it will fail.`,
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

const UnexpectedStep = 26;
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

const TargetWorkFlowCancelled = 27;
export class DBOSAwaitedWorkflowCancelledError extends DBOSError {
  constructor(readonly workflowID: string) {
    super(`Awaited ${workflowID} was cancelled`, TargetWorkFlowCancelled);
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

export function getDBOSErrorCode(e: Error): number | undefined {
  if (e && typeof e === 'object' && 'dbosErrorCode' in e) {
    const code = (e as Record<string, unknown>).dbosErrorCode;
    return typeof code === 'number' ? code : undefined;
  }
  return undefined;
}
