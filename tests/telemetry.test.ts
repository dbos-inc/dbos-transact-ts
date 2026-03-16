import { InMemorySpanExporter, ReadableSpan, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from './nodetraceprovider';
import { DBOS } from '../src';
import Koa from 'koa';
import Router from '@koa/router';
import { context, trace, SpanStatusCode } from '@opentelemetry/api';
import { isTraceContextWorking } from '../src/telemetry/traces';
import { AddressInfo } from 'net';
import { globalParams } from '../src/utils';

async function tracedStep() {
  return Promise.resolve();
}

async function doSomethingTraced_internal() {
  const span = trace.getSpan(context.active());
  if (span) {
    span.setAttribute('my-lib.didSomething', true);
  }
  if (globalParams.tracingEnabled) {
    expect(DBOS.span).toBe(trace.getSpan(context.active()));
  }
  await DBOS.runStep(tracedStep, { name: 'tracedStep' });
  return Promise.resolve('Done');
}

const doSomethingTraced = DBOS.registerWorkflow(doSomethingTraced_internal);

function createApp() {
  const app = new Koa();
  const router = new Router();

  app.use(async (ctx, next) => {
    const current = trace.getSpan(context.active());
    if (current) {
      return next() as Promise<unknown>;
    }

    const tracer = trace.getTracer('manual');
    const span = tracer.startSpan(`manual-span-for-${ctx.method} ${ctx.path}`);

    try {
      await context.with(trace.setSpan(context.active(), span), async () => {
        await next();
        if (ctx.status >= 400) {
          span.setStatus({ code: SpanStatusCode.ERROR, message: ctx.message });
        } else {
          span.setStatus({ code: SpanStatusCode.OK });
        }
      });
    } catch (err) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      throw err;
    } finally {
      span.end();
    }
  });

  router.get('/test', async (ctx) => {
    await doSomethingTraced();
    ctx.body = 'OK';
  });

  app.use(router.routes());
  app.use(router.allowedMethods());

  return app;
}

function getParentSpanID(span: ReadableSpan) {
  const ctx = span.parentSpanContext;
  return ctx ? ctx.spanId : undefined;
}

describe('trace spans propagate', () => {
  const memoryExporter = new InMemorySpanExporter();

  beforeAll(async () => {
    const provider = new NodeTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(memoryExporter)],
    });
    provider.register();
    DBOS.setConfig({ name: 'trace-span-propagate', enableOTLP: true });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
    trace.disable();
    context.disable();
  });

  test('from-outside-into-DBOS-calls', async () => {
    expect(isTraceContextWorking()).toBe(true);

    const app = createApp();
    const server = app.listen(0);

    const { port } = server.address() as AddressInfo;

    const res = await fetch(`http://localhost:${port}/test`);

    expect(res.status).toBe(200);
    server.close();

    const spans = memoryExporter.getFinishedSpans();
    expect(spans.length).toBe(5);

    console.debug(
      spans.map((span) => ({
        name: span.name,
        traceId: span.spanContext().traceId,
        spanId: span.spanContext().spanId,
        parentSpanId: getParentSpanID(span),
        attributes: span.attributes,
      })),
    );

    // First two spans are probes, ignore them
    const stepSpan = spans[2];
    const workflowspan = spans[3];
    const httpSpan = spans[4];
    expect(getParentSpanID(stepSpan)).toBe(workflowspan?.spanContext().spanId);
    expect(stepSpan?.spanContext().traceId).toBe(workflowspan?.spanContext().traceId);
    expect(getParentSpanID(workflowspan)).toBe(httpSpan?.spanContext().spanId);
    expect(workflowspan?.spanContext().traceId).toBe(httpSpan?.spanContext().traceId);
    expect(workflowspan.attributes['my-lib.didSomething']).toBeTruthy();
  });
});

describe('disable-otlp', () => {
  const memoryExporter = new InMemorySpanExporter();

  beforeAll(async () => {
    const provider = new NodeTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(memoryExporter)],
    });
    provider.register();
    DBOS.setConfig({ name: 'trace-span-propagate' });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
    trace.disable();
    context.disable();
  });

  test('disable-otlp', async () => {
    expect(isTraceContextWorking()).toBe(false);

    const app = createApp();
    const server = app.listen(0);

    const { port } = server.address() as AddressInfo;

    const res = await fetch(`http://localhost:${port}/test`);

    expect(res.status).toBe(200);
    server.close();

    // With OTLP disabled, only the HTTP span is present
    const spans = memoryExporter.getFinishedSpans();
    expect(spans.length).toBe(1);
  });
});

describe('external-provider-span-propagation', () => {
  const memoryExporter = new InMemorySpanExporter();

  beforeAll(async () => {
    const provider = new NodeTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(memoryExporter)],
    });
    provider.register();
    DBOS.setConfig({ name: 'external-provider-test', tracingEnabled: true });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
    trace.disable();
    context.disable();
  });

  test('spans-flow-through-external-provider', async () => {
    expect(isTraceContextWorking()).toBe(true);

    const app = createApp();
    const server = app.listen(0);

    const { port } = server.address() as AddressInfo;

    const res = await fetch(`http://localhost:${port}/test`);

    expect(res.status).toBe(200);
    server.close();

    const spans = memoryExporter.getFinishedSpans();
    const realSpans = spans.filter((s) => s.name !== 'probe');
    expect(realSpans.length).toBe(3);

    const stepSpan = realSpans[0];
    const workflowSpan = realSpans[1];
    const httpSpan = realSpans[2];

    expect(getParentSpanID(stepSpan)).toBe(workflowSpan?.spanContext().spanId);
    expect(stepSpan?.spanContext().traceId).toBe(workflowSpan?.spanContext().traceId);
    expect(getParentSpanID(workflowSpan)).toBe(httpSpan?.spanContext().spanId);
    expect(workflowSpan?.spanContext().traceId).toBe(httpSpan?.spanContext().traceId);
  });
});

describe('dbos-standalone-tracing', () => {
  beforeAll(async () => {
    // No external provider — DBOS sets up its own BasicTracerProvider and context manager
    DBOS.setConfig({ name: 'standalone-tracing-test', enableOTLP: true });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
    trace.disable();
    context.disable();
  });

  test('context-propagation-works', () => {
    expect(isTraceContextWorking()).toBe(true);
  });

  test('workflows-produce-real-spans', async () => {
    const result = await doSomethingTraced();
    expect(result).toBe('Done');
  });
});
