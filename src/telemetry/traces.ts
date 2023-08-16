import { BasicTracerProvider, Span } from "@opentelemetry/sdk-trace-base";
import opentelemetry, { HrTime } from "@opentelemetry/api";
import { TelemetryCollector } from "./collector";
import { TelemetrySignal } from "./signals";

// https://github.com/open-telemetry/opentelemetry-js/blob/853a7b6edeb584e800499dbb65a3b42aa45c87e8/packages/opentelemetry-core/src/common/time.ts#L139
function hrTimeToMicroseconds(time: HrTime): number {
  return time[0] * 1e6 + time[1] / 1e3;
}

export class Tracer {
  private readonly tracer: BasicTracerProvider;
  constructor(private readonly telemetryCollector: TelemetryCollector) {
    this.tracer = new BasicTracerProvider();
    this.tracer.register();
  }

  startSpan(name: string, parentSpan?: Span): Span {
    const tracer = opentelemetry.trace.getTracer("default");
    if (parentSpan) {
      const ctx = opentelemetry.trace.setSpan(
        opentelemetry.context.active(),
        parentSpan
      );
      return tracer.startSpan(name, { startTime: Date.now() }, ctx) as Span;
    } else {
      return tracer.startSpan(name) as Span;
    }
  }

  endSpan(span: Span) {
    span.end(Date.now());
    const readableSpan = {
      kind: span.kind,
      traceId: span.spanContext().traceId,
      spanId: span.spanContext().spanId,
      traceFlags: span.spanContext().traceFlags,
      traceState: span.spanContext().traceState?.serialize(),
      parentSpanId: span.parentSpanId,
      name: span.name,
      start: hrTimeToMicroseconds(span.startTime),
      duration: hrTimeToMicroseconds(span.duration),
      attributes: span.attributes,
      status: span.status,
      events: span.events,
    };

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
      traceSpan: JSON.stringify(readableSpan),
    };

    console.log(signal);
    this.telemetryCollector.push(signal);
  }
}
