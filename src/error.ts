import {DatabaseError} from "pg";
import { ExportResult } from "@opentelemetry/core";

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


export class OperonError extends Error {
  // TODO: define a better coding system.
  constructor(msg: string, readonly operonErrorCode: number = 1) {
    super(msg);
  }
}

const WorkflowPermissionDeniedError = 2;
export class OperonWorkflowPermissionDeniedError extends OperonError {
  constructor(runAs: string, workflowName: string) {
    const msg = `Subject ${runAs} does not have permission to run workflow ${workflowName}`;
    super(msg, WorkflowPermissionDeniedError);
  }
}

const InitializationError = 3;
export class OperonInitializationError extends OperonError {
  constructor(msg: string) {
    super(msg, InitializationError);
  }
}

const TopicPermissionDeniedError = 4;
export class OperonTopicPermissionDeniedError extends OperonError {
  constructor(topic: string, workflowUUID: string, functionID: number, runAs: string) {
    const msg =
      `Subject ${runAs} does not have permission on topic ${topic}.`
      + `(workflow UUID: ${workflowUUID}, function ID: ${functionID})`;
    super(msg, TopicPermissionDeniedError);
  }
}

const ConflictingUUIDError = 5;
export class OperonWorkflowConflictUUIDError extends OperonError {
  constructor() {
    super("Conflicting UUIDs", ConflictingUUIDError);
  }
}

const WorkflowUnknownError = 6;
export class OperonWorkflowUnknownError extends OperonError {
  constructor(workflowUUID: string, workflowname: string) {
    const msg =
      `Workflow (UUID: ${workflowUUID} Name: ${workflowname}) unknown during recovery`;
    super(msg, WorkflowUnknownError);
  }
}

const PostgresExporterError = 7;
export class OperonPostgresExporterError extends OperonError {
  constructor(err: Error) {
    let msg = `PostgresExporter error: ${err.message} \n`;
    if (err instanceof DatabaseError) {
      msg = msg.concat(formatPgDatabaseError(err));
    }
    super(msg, PostgresExporterError);
  }
}

const JaegerExporterError = 8;
export class OperonJaegerExporterError extends OperonError {
  constructor(err: ExportResult) {
    let msg = `JaegerExporter error ${err.code}`;
    if (err.error) {
      msg = msg.concat(`: ${err.error.message}`);
    }
    msg = msg.concat(`\n`);
    super(msg, JaegerExporterError);
  }
}

