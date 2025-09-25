import type { Span } from '@opentelemetry/sdk-trace-base';
import { BasicTracerProvider } from '@opentelemetry/sdk-trace-base';
import { Resource } from '@opentelemetry/resources';
import type { SpanContext } from '@opentelemetry/api';
import opentelemetry from '@opentelemetry/api';
import { TelemetryCollector } from './collector';
import { hrTime } from '@opentelemetry/core';
import { globalParams } from '../utils';

import { context, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';

interface Attributes {
  [attributeKey: string]: AttributeValue | undefined;
}
/**
 * Attribute values may be any non-nullish primitive value except an object.
 *
 * null or undefined attribute values are invalid and will result in undefined behavior.
 */
declare type AttributeValue =
  | string
  | number
  | boolean
  | Array<null | undefined | string>
  | Array<null | undefined | number>
  | Array<null | undefined | boolean>;

export enum SpanStatusCode {
  /**
   * The default status.
   */
  UNSET = 0,
  /**
   * The operation has been validated by an Application developer or
   * Operator to have completed successfully.
   */
  OK = 1,
  /**
   * The operation contains an error.
   */
  ERROR = 2,
}

interface SpanStatus {
  /** The status code of this message. */
  code: SpanStatusCode;
  /** A developer-facing error message. */
  message?: string;
}

export type DBOSSpan = {
  setStatus(status: SpanStatus): DBOSSpan;
  attributes: Attributes;
  setAttribute(key: string, attribute: AttributeValue): DBOSSpan;
  addEvent(name: string, attributesOrStartTime?: Attributes, timeStamp?: number): DBOSSpan;
};

export class StubSpan implements DBOSSpan {
  attributes: Attributes = {};

  setStatus(_status: SpanStatus): DBOSSpan {
    return this;
  }

  setAttribute(_key: string, _attribute: AttributeValue): DBOSSpan {
    return this;
  }

  addEvent(_name: string, _attributesOrStartTime?: Attributes, _timeStamp?: number): DBOSSpan {
    return this;
  }
}

export function runWithTrace<R>(span: DBOSSpan, func: () => Promise<R>): Promise<R> {
  if (!globalParams.enableOTLP) {
    return func();
  }
  return context.with(trace.setSpan(context.active(), span as Span), func);
}

export function getActiveSpan() {
  if (!globalParams.enableOTLP) {
    return undefined;
  }
  return trace.getActiveSpan() as DBOSSpan | undefined;
}

export function isTraceContextWorking(): boolean {
  if (!globalParams.enableOTLP) {
    return false;
  }
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
  if (!globalParams.enableOTLP) {
    return;
  }
  const contextManager = new AsyncLocalStorageContextManager();
  contextManager.enable();
  context.setGlobalContextManager(contextManager);

  const provider = new BasicTracerProvider();
  provider.register();
}

export class Tracer {
  readonly applicationID: string;
  readonly executorID: string;
  constructor(private readonly telemetryCollector: TelemetryCollector) {
    this.applicationID = globalParams.appID;
    this.executorID = globalParams.executorID; // for consistency with src/context.ts
    if (!globalParams.enableOTLP) {
      return;
    }
    const tracer = new BasicTracerProvider({
      resource: new Resource({
        'service.name': 'dbos',
      }),
    });
    tracer.register(); // this is a no-op if another tracer provider was already registered
  }

  startSpanWithContext(spanContext: unknown, name: string, attributes?: Attributes): DBOSSpan {
    if (!globalParams.enableOTLP) {
      return new StubSpan();
    }
    const tracer = opentelemetry.trace.getTracer('dbos-tracer');
    const ctx = opentelemetry.trace.setSpanContext(opentelemetry.context.active(), spanContext as SpanContext);
    return tracer.startSpan(name, { startTime: performance.now(), attributes: attributes }, ctx) as Span;
  }

  startSpan(name: string, attributes?: Attributes, inputSpan?: DBOSSpan): DBOSSpan {
    if (!globalParams.enableOTLP) {
      return new StubSpan();
    }
    const parentSpan = inputSpan as Span;
    const tracer = opentelemetry.trace.getTracer('dbos-tracer');
    const startTime = hrTime(performance.now());
    if (parentSpan) {
      const ctx = opentelemetry.trace.setSpan(opentelemetry.context.active(), parentSpan);
      return tracer.startSpan(name, { startTime: startTime, attributes: attributes }, ctx) as Span;
    } else {
      return tracer.startSpan(name, { startTime: startTime, attributes: attributes }) as Span;
    }
  }

  endSpan(inputSpan: DBOSSpan) {
    if (!globalParams.enableOTLP) {
      return;
    }
    const span = inputSpan as Span;
    span.setAttributes({
      applicationID: this.applicationID,
      applicationVersion: globalParams.appVersion,
    });
    if (span.attributes && !('executorID' in span.attributes)) {
      span.setAttribute('executorID', this.executorID);
    }
    span.end(hrTime(performance.now()));
    this.telemetryCollector.push(span);
  }
}
