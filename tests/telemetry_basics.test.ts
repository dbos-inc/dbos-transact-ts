import { context, trace } from '@opentelemetry/api';
import { BasicTracerProvider } from '@opentelemetry/sdk-trace-base';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';

const contextManager = new AsyncLocalStorageContextManager();
contextManager.enable();
context.setGlobalContextManager(contextManager);

const provider = new BasicTracerProvider();
provider.register();

describe('with-traced-context', () => {
  beforeAll(() => {});

  test('simple', async () => {
    const tracer = trace.getTracer('test');

    const span = tracer.startSpan('testspan');

    const r2 = await context.with(trace.setSpan(context.active(), span), async () => {
      expect(trace.getSpan(context.active())).toBe(span);
      return Promise.resolve(2);
    });
    expect(r2).toBe(2);
  });
});
