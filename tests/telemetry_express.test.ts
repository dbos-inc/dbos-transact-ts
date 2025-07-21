import { AlwaysOnSampler, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { registerInstrumentations } from '@opentelemetry/instrumentation';

import { HttpInstrumentation } from '@opentelemetry/instrumentation-http';
import { ExpressInstrumentation } from '@opentelemetry/instrumentation-express';
import { isTraceContextWorking } from '../src/telemetry/traces';

/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { BasicTracerProvider, SDKRegistrationConfig } from '@opentelemetry/sdk-trace-base';
import { TracerConfig as NodeTracerConfig } from '@opentelemetry/sdk-trace-base';
import { context, ContextManager, propagation, SpanStatusCode, TextMapPropagator, trace } from '@opentelemetry/api';
import { CompositePropagator, W3CBaggagePropagator, W3CTraceContextPropagator } from '@opentelemetry/core';

function setupContextManager(contextManager: ContextManager | null | undefined) {
  // null means 'do not register'
  if (contextManager === null) {
    return;
  }

  // undefined means 'register default'
  if (contextManager === undefined) {
    const defaultContextManager = new AsyncLocalStorageContextManager();
    defaultContextManager.enable();
    context.setGlobalContextManager(defaultContextManager);
    return;
  }

  contextManager.enable();
  context.setGlobalContextManager(contextManager);
}

function setupPropagator(propagator: TextMapPropagator | null | undefined) {
  // null means 'do not register'
  if (propagator === null) {
    return;
  }

  // undefined means 'register default'
  if (propagator === undefined) {
    propagation.setGlobalPropagator(
      new CompositePropagator({
        propagators: [new W3CTraceContextPropagator(), new W3CBaggagePropagator()],
      }),
    );
    return;
  }

  propagation.setGlobalPropagator(propagator);
}

/**
 * Register this TracerProvider for use with the OpenTelemetry API.
 * Undefined values may be replaced with defaults, and
 * null values will be skipped.
 *
 * @param config Configuration object for SDK registration
 */
export class NodeTracerProvider extends BasicTracerProvider {
  constructor(config: NodeTracerConfig = {}) {
    super(config);
  }

  /**
   * Register this TracerProvider for use with the OpenTelemetry API.
   * Undefined values may be replaced with defaults, and
   * null values will be skipped.
   *
   * @param config Configuration object for SDK registration
   */
  register(config: SDKRegistrationConfig = {}): void {
    trace.setGlobalTracerProvider(this);
    setupContextManager(config.contextManager);
    setupPropagator(config.propagator);
  }
}

const memoryExporter = new InMemorySpanExporter();

const provider = new NodeTracerProvider({
  sampler: new AlwaysOnSampler(),
});
provider.addSpanProcessor(new SimpleSpanProcessor(memoryExporter));

provider.register();

registerInstrumentations({
  tracerProvider: provider,
  instrumentations: [new ExpressInstrumentation(), new HttpInstrumentation()],
});

import express from 'express';
import request from 'supertest';

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

  app.use(async (req, res, next) => {
    const current = trace.getSpan(context.active());

    if (current) {
      console.log('Had context already...');
      return next();
    }

    const tracer = trace.getTracer('manual');
    const span = tracer.startSpan(`manual-span-for-${req.method} ${req.path}`);

    // Attach span to current context and propagate it
    try {
      await context.with(trace.setSpan(context.active(), span), next as () => Promise<void>);
      if (res.statusCode >= 400) {
        span.setStatus({ code: SpanStatusCode.ERROR, message: res.statusMessage });
      } else {
        span.setStatus({ code: SpanStatusCode.OK });
      }
    } catch (e) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: (e as Error).message });
      throw e;
    } finally {
      span.end();
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

      const res = await fetch(`http://localhost:${port}/test`, {
        headers: {
          traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-abcdefabcdefabcd-01',
        },
      });
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
