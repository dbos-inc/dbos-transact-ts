import { BasicTracerProvider, ReadableSpan, Span } from '@opentelemetry/sdk-trace-base';
import { Resource } from '@opentelemetry/resources';
import opentelemetry, { Attributes, SpanContext } from '@opentelemetry/api';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';
import { TelemetryCollector } from './collector';
import { hrTime } from '@opentelemetry/core';
import { globalParams } from '../utils';

export class Tracer {
  private readonly tracer: BasicTracerProvider;
  readonly applicationID: string;
  readonly executorID: string;
  constructor(private readonly telemetryCollector: TelemetryCollector) {
    this.tracer = new BasicTracerProvider({
      resource: new Resource({
        [SemanticResourceAttributes.SERVICE_NAME]: 'dbos',
      }),
    });
    this.tracer.register(); // this is a no-op if another tracer provider was already registered
    this.applicationID = globalParams.appID;
    this.executorID = globalParams.executorID; // for consistency with src/context.ts
  }

  startSpanWithContext(spanContext: SpanContext, name: string, attributes?: Attributes): Span {
    const tracer = opentelemetry.trace.getTracer('dbos-tracer');
    const ctx = opentelemetry.trace.setSpanContext(opentelemetry.context.active(), spanContext);
    return tracer.startSpan(name, { startTime: performance.now(), attributes: attributes }, ctx) as Span;
  }

  startSpan(name: string, attributes?: Attributes, parentSpan?: Span): Span {
    const tracer = opentelemetry.trace.getTracer('dbos-tracer');
    const startTime = hrTime(performance.now());
    if (parentSpan) {
      const ctx = opentelemetry.trace.setSpan(opentelemetry.context.active(), parentSpan);
      return tracer.startSpan(name, { startTime: startTime, attributes: attributes }, ctx) as Span;
    } else {
      return tracer.startSpan(name, { startTime: startTime, attributes: attributes }) as Span;
    }
  }

  endSpan(span: Span) {
    span.end(hrTime(performance.now()));
    span.setAttributes({
      applicationID: this.applicationID,
      applicationVersion: globalParams.appVersion,
    });
    if (span.attributes && !('executorID' in span.attributes)) {
      span.setAttribute('executorID', this.executorID);
    }
    this.telemetryCollector.push(span as ReadableSpan);
  }
}
