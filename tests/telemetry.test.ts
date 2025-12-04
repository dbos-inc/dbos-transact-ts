import { InMemorySpanExporter, ReadableSpan, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from './nodetraceprovider';
import { DBOS } from '../src';

const memoryExporter = new InMemorySpanExporter();
const provider = new NodeTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(memoryExporter)],
});
provider.register();

import Koa from 'koa';
import Router from '@koa/router';
import { context, trace, SpanStatusCode, SpanContext } from '@opentelemetry/api';
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
  if (globalParams.enableOTLP) {
    expect(DBOS.span).toBe(trace.getSpan(context.active()));
  }
  await DBOS.runStep(tracedStep, { name: 'tracedStep' });
  return Promise.resolve('Done');
}

const doSomethingTraced = DBOS.registerWorkflow(doSomethingTraced_internal);

export function createApp() {
  const app = new Koa();
  const router = new Router();

  // Tracing middleware (emulates instrumentation or full middleware, which is not working...)
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

  // Route
  router.get('/test', async (ctx) => {
    await doSomethingTraced();
    ctx.body = 'OK';
  });

  app.use(router.routes());
  app.use(router.allowedMethods());

  return app;
}

function getParentSpanID(span: ReadableSpan) {
  const ctx = span.parentSpanContext as SpanContext | undefined;
  if (ctx) {
    return ctx.spanId;
  } else {
    return undefined;
  }
}

describe('trace spans propagate ', () => {
  beforeAll(async () => {
    memoryExporter.reset();
    DBOS.setConfig({ name: 'trace-span-propagate', enableOTLP: true });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  test('from-outside-into-DBOS-calls', async () => {
    expect(isTraceContextWorking()).toBe(true);

    const app = createApp();
    const server = app.listen(0); // Koa uses native HTTP

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
  beforeAll(async () => {
    memoryExporter.reset();
    DBOS.setConfig({ name: 'trace-span-propagate' });
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  test('disable-otlp', async () => {
    expect(isTraceContextWorking()).toBe(false);

    const app = createApp();
    const server = app.listen(0); // Koa uses native HTTP

    const { port } = server.address() as AddressInfo;

    const res = await fetch(`http://localhost:${port}/test`);

    expect(res.status).toBe(200);
    server.close();

    // With OTLP disabled, only the HTTP span is present
    const spans = memoryExporter.getFinishedSpans();
    expect(spans.length).toBe(1);
  });
});
