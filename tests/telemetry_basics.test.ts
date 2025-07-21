import { context, trace } from '@opentelemetry/api';
import { installTraceContextManager, isTraceContextWorking } from '../src/telemetry/traces';

// This tests the very basic functioning of the context manager
describe('with-traced-context', () => {
  beforeAll(() => {});

  test('simple', async () => {
    expect(isTraceContextWorking()).toBe(false);
    installTraceContextManager();
    expect(isTraceContextWorking()).toBe(true);

    const tracer = trace.getTracer('test');

    const span = tracer.startSpan('testspan');

    const r2 = await context.with(trace.setSpan(context.active(), span), async () => {
      expect(trace.getSpan(context.active())).toBe(span);
      return Promise.resolve(2);
    });
    expect(r2).toBe(2);
  });
});
