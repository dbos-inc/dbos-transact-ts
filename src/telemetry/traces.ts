import { BasicTracerProvider, ReadableSpan, Span } from '@opentelemetry/sdk-trace-base';
import { Resource } from '@opentelemetry/resources';
import opentelemetry, { Attributes, SpanContext } from '@opentelemetry/api';
import { TelemetryCollector } from './collector';
import { hrTime } from '@opentelemetry/core';
import { globalParams } from '../utils';

import { context, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';

export function isTraceContextWorking(): boolean {
  const span = trace.getTracer('otel-bootstrap-check').startSpan('probe');
  const testContext = trace.setSpan(context.active(), span);

  let visible: boolean | undefined;
  context.with(testContext, () => {
    visible = trace.getSpan(context.active()) === span;
  });

  span.end?.();
  return visible === true;
}

export function installTraceContextManager() {
  const contextManager = new AsyncLocalStorageContextManager();
  contextManager.enable();
  context.setGlobalContextManager(contextManager);

  const provider = new BasicTracerProvider();
  provider.register();
}

export class Tracer {
  private readonly tracer: BasicTracerProvider;
  readonly applicationID: string;
  readonly executorID: string;
  constructor(private readonly telemetryCollector: TelemetryCollector) {
    this.tracer = new BasicTracerProvider({
      resource: new Resource({
        'service.name': 'dbos',
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
    span.setAttributes({
      applicationID: this.applicationID,
      applicationVersion: globalParams.appVersion,
    });
    if (span.attributes && !('executorID' in span.attributes)) {
      span.setAttribute('executorID', this.executorID);
    }
    span.end(hrTime(performance.now()));
    this.telemetryCollector.push(span as ReadableSpan);
  }
}
