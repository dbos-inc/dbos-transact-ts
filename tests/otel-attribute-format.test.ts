/**
 * Tests for the `otelAttributeFormat` configuration flag.
 *
 * When `otelAttributeFormat: 'legacy'` (the default), DBOS span attributes
 * use their original camelCase names. When `otelAttributeFormat: 'semconv'`,
 * they're emitted under the OTel-style `dbos.*` namespace.
 */

import { InMemorySpanExporter, ReadableSpan, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from './nodetraceprovider';
import { context, trace } from '@opentelemetry/api';
import { DBOS } from '../src';
import { Tracer } from '../src/telemetry/traces';
import { TelemetryCollector } from '../src/telemetry/collector';
import { DBOSExecutor } from '../src/dbos-executor';

// Source of truth for the legacy -> semconv mapping. Duplicated from
// `src/telemetry/traces.ts` on purpose: if the production mapping changes,
// this test must change too, which forces the cross-SDK schema to be a
// deliberate edit rather than an accidental drift.
const EXPECTED_LEGACY_TO_SEMCONV: Readonly<Record<string, string>> = {
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

// A representative value for each legacy attribute key, used by the
// end-to-end sweep tests below.
const ALL_LEGACY_ATTRS = {
  operationUUID: 'wf-id',
  operationType: 'workflow',
  operationName: 'test-op',
  applicationID: 'app-id',
  applicationVersion: 'v1',
  executorID: 'ex-id',
  queueName: 'my-queue',
  authenticatedUser: 'alice',
  authenticatedRoles: ['admin'],
  assumedRole: 'admin',
  requestID: 'rid-1',
  requestIP: '1.2.3.4',
  requestURL: '/x',
  requestMethod: 'GET',
};

function findSpanByName(spans: readonly ReadableSpan[], name: string): ReadableSpan | undefined {
  return spans.find((s) => s.name === name);
}

const wf_legacy_internal = async () => Promise.resolve();
const attr_legacy_wf = DBOS.registerWorkflow(wf_legacy_internal, { name: 'attr_legacy_wf' });

const wf_semconv_internal = async () => Promise.resolve();
const attr_semconv_wf = DBOS.registerWorkflow(wf_semconv_internal, { name: 'attr_semconv_wf' });

describe('Tracer.resolveAttributeName', () => {
  let collector: TelemetryCollector;

  beforeAll(() => {
    collector = new TelemetryCollector();
  });

  afterAll(async () => {
    await collector.destroy();
  });

  test('legacy format passes every mapped key through unchanged', () => {
    const tracer = new Tracer(collector, 'legacy');
    for (const legacy of Object.keys(EXPECTED_LEGACY_TO_SEMCONV)) {
      expect(tracer.resolveAttributeName(legacy)).toBe(legacy);
    }
  });

  test('semconv format remaps every mapped key to its dbos.* name', () => {
    const tracer = new Tracer(collector, 'semconv');
    for (const [legacy, semconv] of Object.entries(EXPECTED_LEGACY_TO_SEMCONV)) {
      expect(tracer.resolveAttributeName(legacy)).toBe(semconv);
    }
  });

  test('unknown attribute names pass through in either mode', () => {
    for (const fmt of ['legacy', 'semconv'] as const) {
      const tracer = new Tracer(collector, fmt);
      expect(tracer.resolveAttributeName('custom.user.attribute')).toBe('custom.user.attribute');
      expect(tracer.resolveAttributeName('cached')).toBe('cached');
    }
  });
});

describe('otelAttributeFormat: legacy (default)', () => {
  const memoryExporter = new InMemorySpanExporter();

  beforeAll(async () => {
    const provider = new NodeTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(memoryExporter)],
    });
    provider.register();
    DBOS.setConfig({ name: 'attr-format-legacy', enableOTLP: true });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
    trace.disable();
    context.disable();
  });

  test('workflow span carries legacy attribute names', async () => {
    memoryExporter.reset();

    await attr_legacy_wf();

    const wfSpan = findSpanByName(memoryExporter.getFinishedSpans(), 'attr_legacy_wf');
    expect(wfSpan).toBeDefined();
    const attrs = wfSpan!.attributes;
    expect(attrs.applicationVersion).toBeDefined();
    expect(attrs.executorID).toBeDefined();
    expect(attrs.applicationID).toBeDefined();
    expect(attrs['dbos.application.version']).toBeUndefined();
    expect(attrs['dbos.executor.id']).toBeUndefined();
  });

  test('startSpan emits every mapped legacy key under its original name', () => {
    memoryExporter.reset();

    const tracer = DBOSExecutor.globalInstance!.tracer;
    const span = tracer.startSpan('all-attrs-legacy', { ...ALL_LEGACY_ATTRS });
    tracer.endSpan(span);

    const captured = findSpanByName(memoryExporter.getFinishedSpans(), 'all-attrs-legacy');
    expect(captured).toBeDefined();
    const attrs = captured!.attributes;

    for (const legacy of Object.keys(EXPECTED_LEGACY_TO_SEMCONV)) {
      expect(attrs[legacy]).toBeDefined();
    }
    for (const semconv of Object.values(EXPECTED_LEGACY_TO_SEMCONV)) {
      expect(attrs[semconv]).toBeUndefined();
    }
  });
});

describe('otelAttributeFormat: semconv', () => {
  const memoryExporter = new InMemorySpanExporter();

  beforeAll(async () => {
    const provider = new NodeTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(memoryExporter)],
    });
    provider.register();
    DBOS.setConfig({ name: 'attr-format-semconv', enableOTLP: true, otelAttributeFormat: 'semconv' });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
    trace.disable();
    context.disable();
  });

  test('workflow span carries dbos.* names instead of legacy ones', async () => {
    memoryExporter.reset();

    await attr_semconv_wf();

    const wfSpan = findSpanByName(memoryExporter.getFinishedSpans(), 'attr_semconv_wf');
    expect(wfSpan).toBeDefined();
    const attrs = wfSpan!.attributes;
    // semconv names present
    expect(attrs['dbos.application.version']).toBeDefined();
    expect(attrs['dbos.executor.id']).toBeDefined();
    expect(attrs['dbos.application.id']).toBeDefined();
    expect(attrs['dbos.operation.workflow_id']).toBeDefined();
    expect(attrs['dbos.operation.type']).toBe('workflow');
    // legacy names absent
    expect(attrs.applicationVersion).toBeUndefined();
    expect(attrs.executorID).toBeUndefined();
    expect(attrs.applicationID).toBeUndefined();
    expect(attrs.operationUUID).toBeUndefined();
    expect(attrs.operationType).toBeUndefined();
  });

  test('startSpan remaps every mapped legacy key to its dbos.* name', () => {
    memoryExporter.reset();

    const tracer = DBOSExecutor.globalInstance!.tracer;
    const span = tracer.startSpan('all-attrs-semconv', { ...ALL_LEGACY_ATTRS });
    tracer.endSpan(span);

    const captured = findSpanByName(memoryExporter.getFinishedSpans(), 'all-attrs-semconv');
    expect(captured).toBeDefined();
    const attrs = captured!.attributes;

    for (const [legacy, semconv] of Object.entries(EXPECTED_LEGACY_TO_SEMCONV)) {
      expect(attrs[legacy]).toBeUndefined();
      expect(attrs[semconv]).toBeDefined();
    }
  });
});
