import { ReadableSpan } from "@opentelemetry/sdk-trace-base";

export type OperonSignal = TelemetrySignal | ProvenanceSignal;

export interface TelemetrySignal {
  workflowUUID: string;
  functionID: number;
  operationName: string;
  runAs: string;
  timestamp: number;
  severity?: string;
  logMessage?: string;
  traceID?: string;
  traceSpan?: ReadableSpan;
}

export interface ProvenanceSignal {
  transactionID: string;
  kind: string;
  schema: string;
  table: string;
  columnnames: string[];
  columntypes: string[];
  columnvalues: string[];
}