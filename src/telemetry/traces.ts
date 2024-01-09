import { BasicTracerProvider, ReadableSpan, Span } from "@opentelemetry/sdk-trace-base";
import { Resource } from "@opentelemetry/resources";
import opentelemetry, { Attributes, SpanContext } from "@opentelemetry/api";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";
import { TelemetryCollector } from "./collector";

export class Tracer {
  private readonly tracer: BasicTracerProvider;
  readonly applicationId: string;
  readonly vmId: string;
  constructor(private readonly telemetryCollector: TelemetryCollector) {
    this.tracer = new BasicTracerProvider({
      resource: new Resource({
        [SemanticResourceAttributes.SERVICE_NAME]: "dbos",
      }),
    });
    this.tracer.register();
    this.applicationId = process.env.APPID || "APP_ID_NOT_DEFINED";
    this.vmId = process.env.VMID || "VM_ID_NOT_DEFINED";
  }

  startSpanWithContext(spanContext: SpanContext, name: string, attributes?: Attributes): Span {
    const tracer = opentelemetry.trace.getTracer("dbos-tracer");
    const ctx = opentelemetry.trace.setSpanContext(opentelemetry.context.active(), spanContext);
    return tracer.startSpan(name, { startTime: Date.now(), attributes: attributes }, ctx) as Span;
  }

  startSpan(name: string, attributes?: Attributes, parentSpan?: Span): Span {
    const tracer = opentelemetry.trace.getTracer("dbos-tracer");
    if (parentSpan) {
      const ctx = opentelemetry.trace.setSpan(opentelemetry.context.active(), parentSpan);
      return tracer.startSpan(name, { startTime: Date.now(), attributes: attributes }, ctx) as Span;
    } else {
      return tracer.startSpan(name, { attributes: attributes }) as Span;
    }
  }

  endSpan(span: Span) {
    span.end(Date.now());
    span.attributes.applicationId = this.applicationId;
    span.attributes.vmId = this.vmId;
    this.telemetryCollector.push(span as ReadableSpan);
  }
}
