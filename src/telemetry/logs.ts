import { WorkflowContext } from "../workflow.js";
import { TelemetryCollector } from "./collector.js";
import { TelemetrySignal } from "./signals.js";
import { TransactionContext } from "../transaction.js";
import { CommunicatorContext } from "../communicator.js";

interface ILogger {
  log(context: WorkflowContext | TransactionContext, severity: string, message: string): void;
  collector: TelemetryCollector;
}

export class Logger implements ILogger {
  constructor(readonly collector: TelemetryCollector) {}

  log(context: WorkflowContext | TransactionContext | CommunicatorContext, severity: string, message: string): void {
    const signal: TelemetrySignal = {
      workflowUUID: context.workflowUUID,
      functionID: context.functionID,
      operationName: context.operationName,
      runAs: context.runAs,
      timestamp: Date.now(),
      severity: severity,
      logMessage: message,
    };
    this.collector.push(signal);
  }
}
