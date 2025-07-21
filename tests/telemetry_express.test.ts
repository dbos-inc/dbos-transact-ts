import {
  AlwaysOnSampler,
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { registerInstrumentations } from '@opentelemetry/instrumentation';

import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';

import { HttpInstrumentation } from '@opentelemetry/instrumentation-http';
import { ExpressInstrumentation } from '@opentelemetry/instrumentation-express';
import { isTraceContextWorking } from '../src/telemetry/traces';

const memoryExporter = new InMemorySpanExporter();

const provider = new BasicTracerProvider({
  sampler: new AlwaysOnSampler(),
});
provider.addSpanProcessor(new SimpleSpanProcessor(memoryExporter));

provider.register({
  contextManager: new AsyncLocalStorageContextManager().enable(),
});

registerInstrumentations({
  tracerProvider: provider,
  instrumentations: [new HttpInstrumentation(), new ExpressInstrumentation()],
});

import express from 'express';
import request from 'supertest';
import { context, trace } from '@opentelemetry/api';

async function doSomethingTraced() {
  const span = trace.getSpan(context.active());
  console.log('Current span:', span?.spanContext());
  if (span) {
    span.setAttribute('my-lib.didSomething', true);
  }
  return Promise.resolve('Done');
}

export function createApp() {
  const app = express();

  app.use((req, res, next) => {
    const current = trace.getSpan(context.active());
    if (!current) {
      const tracer = trace.getTracer('manual');
      const span = tracer.startSpan(`manual-span-for-${req.path}`);
      context.with(trace.setSpan(context.active(), span), () => {
        console.log('🔵 Manually entered span context:', span.spanContext());
        next();
        span.end(); // ⚠️ careful — call after response in real apps
      });
    } else {
      console.log('Had context already...');
      next();
    }
  });

  app.get('/test', async (req, res) => {
    await doSomethingTraced();
    res.send('OK');
  });

  return app;
}

// This tests the very basic functioning of the context manager
describe('with-traced-context', () => {
  beforeEach(() => {
    memoryExporter.reset();
  });

  test('trace spans propagate from HTTP to library', async () => {
    expect(isTraceContextWorking()).toBe(true);

    if (0) {
      const app = createApp();
      const res = await request(app)
        .get('/test')
        .set('traceparent', '00-4bf92f3577b34da6a3ce929d0e0e4736-abcdefabcdefabcd-01');
      expect(res.status).toBe(200);
    } else {
      const app = createApp();
      const server = app.listen(0); // dynamic port
      const address = server.address();
      const port = typeof address === 'string' ? address : address?.port;

      const res = await fetch(`http://localhost:${port}/test`);
      expect(res.status).toBe(200);
      server.close();
    }

    const spans = memoryExporter.getFinishedSpans();
    expect(spans.length).toBeGreaterThan(0);
    console.log(
      spans.map((span) => ({
        name: span.name,
        traceId: span.spanContext().traceId,
        spanId: span.spanContext().spanId,
        parentSpanId: span.parentSpanId,
        attributes: span.attributes,
        events: span.events,
      })),
    );

    const httpSpan = spans.find((s) => s.name.includes('/test'));
    const libSpan = spans.find((s) => s.attributes['my-lib.didSomething'] === true);

    expect(httpSpan).toBeDefined();
    expect(libSpan).toBeDefined();

    //expect(libSpan?.parentSpanId).toBe(httpSpan?.spanContext().spanId);
  });
});
