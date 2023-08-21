import { WorkflowContext } from "./../workflow";
import { TelemetryCollector } from "./collector";
import { TelemetrySignal } from "./signals";
import { TransactionContext } from "./../transaction";
import { CommunicatorContext } from "src/communicator";

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
