import { DatabaseError } from 'pg';

function formatPgDatabaseError(err: DatabaseError): string {
  let msg = '';
  if (err.severity) {
    msg = msg.concat(`severity: ${err.severity} \n`);
  }
  if (err.code) {
    msg = msg.concat(`code: ${err.code} \n`);
  }
  if (err.detail) {
    msg = msg.concat(`detail: ${err.detail} \n`);
  }
  if (err.hint) {
    msg = msg.concat(`hint: ${err.hint} \n`);
  }
  if (err.position) {
    msg = msg.concat(`position: ${err.position} \n`);
  }
  if (err.internalPosition) {
    msg = msg.concat(`internalPosition: ${err.internalPosition} \n`);
  }
  if (err.internalQuery) {
    msg = msg.concat(`internalQuery: ${err.internalQuery} \n`);
  }
  if (err.where) {
    msg = msg.concat(`where: ${err.where} \n`);
  }
  if (err.schema) {
    msg = msg.concat(`schema: ${err.schema} \n`);
  }
  if (err.table) {
    msg = msg.concat(`table: ${err.table} \n`);
  }
  if (err.column) {
    msg = msg.concat(`column: ${err.column} \n`);
  }
  if (err.dataType) {
    msg = msg.concat(`dataType: ${err.dataType} \n`);
  }
  if (err.constraint) {
    msg = msg.concat(`constraint: ${err.constraint} \n`);
  }
  if (err.file) {
    msg = msg.concat(`file: ${err.file} \n`);
  }
  if (err.line) {
    msg = msg.concat(`line: ${err.line} \n`);
  }
  return msg;
}

// Return if the error is caused by client request or by server internal.
export function isClientError(dbosErrorCode: number) {
  return (
    dbosErrorCode === DataValidationError ||
    dbosErrorCode === ConflictingWFIDError ||
    dbosErrorCode === NotRegisteredError ||
    dbosErrorCode === ConflictingWorkflowError
  );
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

//const WorkflowPermissionDeniedError = 2;

const InitializationError = 3;
export class DBOSInitializationError extends DBOSError {
  constructor(msg: string) {
    super(msg, InitializationError);
  }
}

//const TopicPermissionDeniedError = 4;

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

//const PostgresExporterError = 7;

const DataValidationError = 9;
export class DBOSDataValidationError extends DBOSError {
  constructor(msg: string) {
    super(msg, DataValidationError);
  }
}

const ResponseError = 11;
/**
 * This error can be thrown by DBOS applications to indicate
 *  the HTTP response code, in addition to the message.
 */
export class DBOSResponseError extends DBOSError {
  constructor(
    msg: string,
    readonly status: number = 500,
  ) {
    super(msg, ResponseError);
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

//const UndefinedDecoratorInputError = 13;

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

const DeadLetterQueueError = 18;
export class DBOSDeadLetterQueueError extends DBOSError {
  constructor(workflowID: string, maxRetries: number) {
    super(
      `Workflow ${workflowID} has been moved to the dead-letter queue after exceeding the maximum of ${maxRetries} retries`,
      DeadLetterQueueError,
    );
  }
}

const FailedSqlTransactionError = 19;
export class DBOSFailedSqlTransactionError extends DBOSError {
  constructor(workflowID: string, txnName: string) {
    super(`Postgres aborted the ${txnName} transaction of Workflow ${workflowID}.`, FailedSqlTransactionError);
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
      `During execution of workflow ${workflowID} step ${stepID}, function ${recordedName} was recorded when ${expectedName} was expected. Check that your workflow is deterministic.`,
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

const QueueDedupIDDuplicated = 28;
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
