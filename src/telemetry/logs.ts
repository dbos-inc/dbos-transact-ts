import { TelemetryCollector } from "./collector";
import { TelemetrySignal } from "./signals";
import { OperonContext } from "src/context";

interface ILogger {
  log(context: OperonContext, severity: string, message: string): void;
  collector: TelemetryCollector;
}

export class Logger implements ILogger {
  constructor(readonly collector: TelemetryCollector) {}

  log(context: OperonContext, severity: string, message: string): void {
    const signal: TelemetrySignal = {
      workflowUUID: context.workflowUUID,
      operationName: context.operationName,
      runAs: context.authenticatedUser,
      timestamp: Date.now(),
      severity: severity,
      logMessage: message,
    };
    this.collector.push(signal);
  }
}
