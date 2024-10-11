import { DatabaseError } from "pg";

function formatPgDatabaseError(err: DatabaseError): string {
  let msg = "";
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
  return (dbosErrorCode === DataValidationError) || (dbosErrorCode === WorkflowPermissionDeniedError) || (dbosErrorCode === TopicPermissionDeniedError) || (dbosErrorCode === ConflictingUUIDError) || (dbosErrorCode === NotRegisteredError);
}

export class DBOSError extends Error {
  // TODO: define a better coding system.
  constructor(msg: string, readonly dbosErrorCode: number = 1) {
    super(msg);
  }
}

const WorkflowPermissionDeniedError = 2;
export class DBOSWorkflowPermissionDeniedError extends DBOSError {
  constructor(runAs: string, workflowName: string) {
    const msg = `Subject ${runAs} does not have permission to run workflow ${workflowName}`;
    super(msg, WorkflowPermissionDeniedError);
  }
}

const InitializationError = 3;
export class DBOSInitializationError extends DBOSError {
  constructor(msg: string) {
    super(msg, InitializationError);
  }
}

const TopicPermissionDeniedError = 4;
export class DBOSTopicPermissionDeniedError extends DBOSError {
  constructor(destinationUUID: string, workflowUUID: string, functionID: number, runAs: string) {
    const msg = `Subject ${runAs} does not have permission on destination UUID ${destinationUUID}.` + `(workflow UUID: ${workflowUUID}, function ID: ${functionID})`;
    super(msg, TopicPermissionDeniedError);
  }
}

const ConflictingUUIDError = 5;
export class DBOSWorkflowConflictUUIDError extends DBOSError {
  constructor(workflowUUID: string) {
    super(`Conflicting UUID ${workflowUUID}`, ConflictingUUIDError);
  }
}

const NotRegisteredError = 6;
export class DBOSNotRegisteredError extends DBOSError {
  constructor(name: string) {
    const msg = `Operation (Name: ${name}) not registered`;
    super(msg, NotRegisteredError);
  }
}

const PostgresExporterError = 7;
export class DBOSPostgresExporterError extends DBOSError {
  constructor(err: Error) {
    let msg = `PostgresExporter error: ${err.message} \n`;
    if (err instanceof DatabaseError) {
      msg = msg.concat(formatPgDatabaseError(err));
    }
    super(msg, PostgresExporterError);
  }
}

const DataValidationError = 9;
export class DBOSDataValidationError extends DBOSError {
  constructor(msg: string) {
    super(msg, DataValidationError);
  }
}

// This error is thrown by applications.
const ResponseError = 11;
export class DBOSResponseError extends DBOSError {
  constructor(msg: string, readonly status: number = 500) {
    super(msg, ResponseError);
  }
}

const NotAuthorizedError = 12;
export class DBOSNotAuthorizedError extends DBOSError {
  constructor(msg: string, readonly status: number = 403) {
    super(msg, NotAuthorizedError);
  }
}

const UndefinedDecoratorInputError = 13;
export class DBOSUndefinedDecoratorInputError extends DBOSError {
  constructor(decoratorName: string) {
    super(`${decoratorName} received undefined input. Possible circular dependency?`, UndefinedDecoratorInputError);
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
    super("DEBUGGER: " + msg, DebuggerError);
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
  constructor(workflowUUID: string, maxRetries: number) {
    super(`Workflow ${workflowUUID} has been moved to the dead-letter queue after exceeding the maximum of ${maxRetries} retries`, DeadLetterQueueError);
  }
}

const FailedSqlTransactionError = 19;
export class DBOSFailedSqlTransactionError extends DBOSError {
  constructor(workflowUUID: string, txnName: string) {
    super(`Postgres aborted the ${txnName} transaction of Workflow ${workflowUUID}.`, FailedSqlTransactionError);
  }
}
