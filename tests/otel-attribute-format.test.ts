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

  test('legacy format passes legacy keys through unchanged', () => {
    const tracer = new Tracer(collector, 'legacy');
    expect(tracer.resolveAttributeName('operationUUID')).toBe('operationUUID');
    expect(tracer.resolveAttributeName('applicationID')).toBe('applicationID');
    expect(tracer.resolveAttributeName('requestMethod')).toBe('requestMethod');
  });

  test('semconv format remaps legacy keys to dbos.* names', () => {
    const tracer = new Tracer(collector, 'semconv');
    expect(tracer.resolveAttributeName('operationUUID')).toBe('dbos.operation.workflow_id');
    expect(tracer.resolveAttributeName('applicationID')).toBe('dbos.application.id');
    expect(tracer.resolveAttributeName('applicationVersion')).toBe('dbos.application.version');
    expect(tracer.resolveAttributeName('executorID')).toBe('dbos.executor.id');
    expect(tracer.resolveAttributeName('queueName')).toBe('dbos.queue.name');
    expect(tracer.resolveAttributeName('authenticatedUser')).toBe('dbos.user.name');
    expect(tracer.resolveAttributeName('authenticatedRoles')).toBe('dbos.user.roles');
    expect(tracer.resolveAttributeName('assumedRole')).toBe('dbos.user.assumed_role');
    expect(tracer.resolveAttributeName('requestID')).toBe('dbos.request.id');
    expect(tracer.resolveAttributeName('requestMethod')).toBe('dbos.request.method');
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
});
