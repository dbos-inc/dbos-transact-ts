import { BasicTracerProvider, ReadableSpan, Span } from "@opentelemetry/sdk-trace-base";
import { Resource } from "@opentelemetry/resources";
import opentelemetry from "@opentelemetry/api";
import { hrTimeToMicroseconds } from "@opentelemetry/core";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";
import { TelemetryCollector } from "./collector";
import { TelemetrySignal } from "./signals";

export function spanToString(span: ReadableSpan): string {
  return JSON.stringify({
    name: span.name,
    kind: span.kind,
    traceId: span.spanContext().traceId,
    spanId: span.spanContext().spanId,
    traceFlags: span.spanContext().traceFlags,
    traceState: span.spanContext().traceState?.serialize(),
    parentSpanId: span.parentSpanId,
    start: hrTimeToMicroseconds(span.startTime),
    duration: hrTimeToMicroseconds(span.duration),
    attributes: span.attributes,
    status: span.status,
    events: span.events,
  });
}

export class Tracer {
  private readonly tracer: BasicTracerProvider;
  constructor(private readonly telemetryCollector: TelemetryCollector) {
    this.tracer = new BasicTracerProvider({
      resource: new Resource({
        [SemanticResourceAttributes.SERVICE_NAME]: "operon",
      }),
    });
    this.tracer.register();
  }

  startSpan(name: string, parentSpan?: Span): Span {
    const tracer = opentelemetry.trace.getTracer("operon-tracer");
    if (parentSpan) {
      const ctx = opentelemetry.trace.setSpan(opentelemetry.context.active(), parentSpan);
      return tracer.startSpan(name, { startTime: Date.now() }, ctx) as Span;
    } else {
      return tracer.startSpan(name) as Span;
    }
  }

  endSpan(span: Span) {
    span.end(Date.now());
    const readableSpan: ReadableSpan = span as ReadableSpan;

    const workflowUUID = span.attributes.workflowUUID as string;
    const functionID = span.attributes.functionID as number;
    const operationName = span.attributes.operationName as string;
    const runAs = span.attributes.runAs as string;
    const traceID = span.spanContext().traceId;

    const signal: TelemetrySignal = {
      workflowUUID,
      functionID,
      operationName,
      runAs,
      timestamp: Date.now(),
      traceID,
      traceSpan: readableSpan,
    };

    this.telemetryCollector.push(signal);
  }
}
