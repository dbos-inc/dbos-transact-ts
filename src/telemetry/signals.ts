export interface TelemetrySignal {
  workflowUUID: string;
  functionID: number;
  operationName: string;
  runAs: string;
  timestamp: number;
  severity?: string;
  logMessage?: string;
  traceID?: string;
  traceSpan?: string;
}
