import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { ValuesOf } from "../utils";

export type OperonSignal = TelemetrySignal | ProvenanceSignal;

export const LogSeverity = {
  Debug: "DEBUG",
  Info: "INFO",
  Warn: "WARN",
  Error: "ERROR",
  Log: "LOG",
} as const;
export type LogSeverity = ValuesOf<typeof LogSeverity>;

export interface TelemetrySignal {
  workflowUUID: string;
  operationName: string;
  runAs: string;
  timestamp: number;
  transactionID?: string;
  traceID?: string;
  traceSpan?: ReadableSpan;
  severity?: LogSeverity;
  logMessage?: string;
  stack?: string;
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
