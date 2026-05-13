/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-require-imports */
import type { Span } from '@opentelemetry/sdk-trace-base';
import type { SpanContext } from '@opentelemetry/api';
import { TelemetryCollector } from './collector';
import { globalParams } from '../utils';
import type { BasicTracerProvider as BasicTracerProviderType } from '@opentelemetry/sdk-trace-base';
import type { OtelAttributeFormat } from '../dbos-executor';

// Legacy DBOS attribute name -> OpenTelemetry semconv-style equivalent.
// Names align with `dbos-transact-py` (Python SDK) so the two SDKs converge
// on the same attribute schema once `otelAttributeFormat: 'semconv'` is
// selected on both sides.
//
// `startSpan` / `startSpanWithContext` sweep the supplied attributes dict
// through `resolveAttributeName`, so call sites can pass legacy DBOS keys
// directly and have them remapped automatically when
// `otelAttributeFormat === 'semconv'`. Keys not in this table pass through
// unchanged. `endSpan` does the same for the few attributes it sets after
// the span has been created. This mirrors the equivalent loop in
// `dbos-transact-py`'s `start_span`.
const LEGACY_TO_SEMCONV: Readonly<Record<string, string>> = {
  operationUUID: 'dbos.operation.workflow_id',
  operationType: 'dbos.operation.type',
  operationName: 'dbos.operation.name',
  applicationID: 'dbos.application.id',
  applicationVersion: 'dbos.application.version',
  executorID: 'dbos.executor.id',
  queueName: 'dbos.queue.name',
  authenticatedUser: 'dbos.user.name',
  authenticatedRoles: 'dbos.user.roles',
  assumedRole: 'dbos.user.assumed_role',
  requestID: 'dbos.request.id',
  requestIP: 'dbos.request.ip',
  requestURL: 'dbos.request.url',
  requestMethod: 'dbos.request.method',
};

// As DBOS OTLP is optional, OTLP objects must only be dynamically imported
// and only when OTLP is enabled. Importing OTLP types is fine as long
// as signatures using those types are not exported from this file.

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

class StubSpan implements DBOSSpan {
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
  if (!globalParams.tracingEnabled) {
    return func();
  }
  const { context, trace } = require('@opentelemetry/api');
  return context.with(trace.setSpan(context.active(), span as Span), func);
}

export function getActiveSpan() {
  if (!globalParams.tracingEnabled) {
    return undefined;
  }
  const { trace } = require('@opentelemetry/api');
  return trace.getActiveSpan() as DBOSSpan | undefined;
}

export function isTraceContextWorking(): boolean {
  if (!globalParams.tracingEnabled) {
    return false;
  }
  const { context, trace } = require('@opentelemetry/api');
  const span = trace.getTracer('otel-bootstrap-check').startSpan('probe');
  const testContext = trace.setSpan(context.active(), span);

  let visible: boolean | undefined;
  context.with(testContext, () => {
    visible = trace.getSpan(context.active()) === span;
  });

  span.end?.();
  return visible === true;
}

export function installTraceContextManager(appName: string = 'dbos'): void {
  if (!globalParams.tracingEnabled) {
    return;
  }
  const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
  const { context, trace } = require('@opentelemetry/api');
  const { BasicTracerProvider } = require('@opentelemetry/sdk-trace-base');

  // setGlobalTracerProvider and setGlobalContextManager are "first one wins."
  // If an external provider is already registered, these calls are safely ignored.
  const contextManager = new AsyncLocalStorageContextManager();
  contextManager.enable();
  context.setGlobalContextManager(contextManager);
  const provider: BasicTracerProviderType = new BasicTracerProvider({
    resource: {
      attributes: {
        'service.name': appName,
      },
    },
  });
  trace.setGlobalTracerProvider(provider);
}

export class Tracer {
  readonly applicationID: string;
  readonly executorID: string;
  private readonly otelAttributeFormat: OtelAttributeFormat;

  constructor(
    private readonly telemetryCollector: TelemetryCollector,
    otelAttributeFormat: OtelAttributeFormat = 'legacy',
  ) {
    this.applicationID = globalParams.appID;
    this.executorID = globalParams.executorID;
    this.otelAttributeFormat = otelAttributeFormat;
  }

  /**
   * Map a legacy DBOS attribute name to the name that should be emitted on
   * the span, per `otelAttributeFormat`. Returns the original key for
   * unknown attributes.
   *
   * Attributes passed into `startSpan` / `startSpanWithContext` are remapped
   * automatically via this method, so call sites don't need to invoke it
   * directly. Exposed for code paths that write attributes after span
   * creation (e.g. `endSpan`, ad-hoc `setAttribute` calls).
   */
  resolveAttributeName(key: string): string {
    if (this.otelAttributeFormat === 'semconv') {
      return LEGACY_TO_SEMCONV[key] ?? key;
    }
    return key;
  }

  private remapAttributes(attributes?: Attributes): Attributes | undefined {
    if (!attributes || this.otelAttributeFormat !== 'semconv') {
      return attributes;
    }
    const remapped: Attributes = {};
    for (const [k, v] of Object.entries(attributes)) {
      remapped[LEGACY_TO_SEMCONV[k] ?? k] = v;
    }
    return remapped;
  }

  startSpanWithContext(spanContext: unknown, name: string, attributes?: Attributes): DBOSSpan {
    if (!globalParams.tracingEnabled) {
      return new StubSpan();
    }
    const opentelemetry = require('@opentelemetry/api');
    const tracer = opentelemetry.trace.getTracer('dbos-tracer');
    const ctx = opentelemetry.trace.setSpanContext(opentelemetry.context.active(), spanContext as SpanContext);
    return tracer.startSpan(
      name,
      { startTime: performance.now(), attributes: this.remapAttributes(attributes) },
      ctx,
    ) as Span;
  }

  startSpan(name: string, attributes?: Attributes, inputSpan?: DBOSSpan): DBOSSpan {
    if (!globalParams.tracingEnabled) {
      return new StubSpan();
    }
    const parentSpan = inputSpan as Span;
    const opentelemetry = require('@opentelemetry/api');
    const { hrTime } = require('@opentelemetry/core');
    const tracer = opentelemetry.trace.getTracer('dbos-tracer');
    const startTime = hrTime(performance.now());
    const remapped = this.remapAttributes(attributes);
    if (parentSpan) {
      const ctx = opentelemetry.trace.setSpan(opentelemetry.context.active(), parentSpan);
      return tracer.startSpan(name, { startTime: startTime, attributes: remapped }, ctx) as Span;
    } else {
      return tracer.startSpan(name, { startTime: startTime, attributes: remapped }) as Span;
    }
  }

  endSpan(inputSpan: DBOSSpan) {
    if (!globalParams.tracingEnabled) {
      return;
    }
    const { hrTime } = require('@opentelemetry/core');
    const span = inputSpan as Span;
    span.setAttributes({
      [this.resolveAttributeName('applicationID')]: this.applicationID,
      [this.resolveAttributeName('applicationVersion')]: globalParams.appVersion,
    });
    const executorIDKey = this.resolveAttributeName('executorID');
    if (span.attributes && !(executorIDKey in span.attributes)) {
      span.setAttribute(executorIDKey, this.executorID);
    }
    span.end(hrTime(performance.now()));
    // Only push to DBOS's own collector when DBOS manages export.
    // When an external TracerProvider is used, span.end() triggers its processors.
    if (globalParams.enableOTLP) {
      this.telemetryCollector.push(span);
    }
  }
}
