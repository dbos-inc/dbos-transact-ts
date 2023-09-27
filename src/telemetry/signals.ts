import { ReadableSpan } from "@opentelemetry/sdk-trace-base";

export type OperonSignal = TelemetrySignal | ProvenanceSignal;

export interface TelemetrySignal {
  workflowUUID: string;
  operationName: string;
  runAs: string;
  timestamp: number;
  transactionID?: string;
  severity?: string;
  logMessage?: string;
  traceID?: string;
  traceSpan?: ReadableSpan;
}

export interface ProvenanceSignal {
  provTransactionID: string;
  kind: string;
  schema: string;
  table: string;
  columnnames: string[];
  columntypes: string[];
  columnvalues: string[];
}
