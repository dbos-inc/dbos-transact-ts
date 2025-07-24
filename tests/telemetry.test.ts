import { TRACE_PARENT_HEADER, TRACE_STATE_HEADER } from '@opentelemetry/core';
import { DBOSExecutor, DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import request from 'supertest';
import { DBOS } from '../src';
import { context, trace } from '@opentelemetry/api';
import type { Span as SDKSpan } from '@opentelemetry/sdk-trace-base';
import { translateDbosConfig } from '../src/dbos-runtime/config';

export class TestClass {
  @DBOS.step({})
  static async test_function(name: string): Promise<string> {
    expect(trace.getSpan(context.active())).toBeDefined();
    expect((trace.getSpan(context.active()) as SDKSpan).name).toBe('test_function');
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.workflow()
  static async test_workflow(name: string): Promise<string> {
    expect(trace.getSpan(context.active())).toBeDefined();
    expect((trace.getSpan(context.active()) as SDKSpan).name).toBe('test_workflow');

    const funcResult = await TestClass.test_function(name);
    return funcResult;
  }
}

describe('dbos-telemetry', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('DBOS init works with exporters', async () => {
    const dbosConfig = generateDBOSTestConfig('pg-node');
    dbosConfig.otlpLogsEndpoints = ['http://localhost:4317/v1/logs'];
    dbosConfig.otlpTracesEndpoints = ['http://localhost:4317/v1/traces'];

    const internalConfig = translateDbosConfig(dbosConfig);
    expect(internalConfig.telemetry).not.toBeUndefined();
    expect(internalConfig.telemetry?.OTLPExporter?.logsEndpoint).toEqual(dbosConfig.otlpLogsEndpoints);
    expect(internalConfig.telemetry?.OTLPExporter?.tracesEndpoint).toEqual(dbosConfig.otlpTracesEndpoints);

    await setUpDBOSTestDb(dbosConfig);
    const dbosExec = new DBOSExecutor(internalConfig);
    expect(dbosExec.telemetryCollector).not.toBeUndefined();
    expect(dbosExec.telemetryCollector.exporter).not.toBeUndefined();
    await dbosExec.init();
    await dbosExec.destroy();
  });

  // TODO write a test intercepting OTLP over HTTP requests and test span/logs payloads

  describe('http Tracer', () => {
    let config: DBOSConfig;

    beforeAll(async () => {
      config = generateDBOSTestConfig('pg-node');
      await setUpDBOSTestDb(config);
      DBOS.setConfig(config);
    });

    beforeEach(async () => {
      await DBOS.launch();
    });

    afterEach(async () => {
      await DBOS.shutdown();
    });

    test('Trace context is propagated in and out of workflow execution', async () => {
      const headers = {
        [TRACE_PARENT_HEADER]: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
        [TRACE_STATE_HEADER]: 'some_state=some_value',
      };

      const response = await request(DBOS.getHTTPHandlersCallback()!).get('/hello').set(headers);
      expect(response.statusCode).toBe(200);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      expect(response.body.message).toBe('hello joe');
      // traceId should be the same, spanId should be different (ID of the last operation's span)
      expect(response.headers.traceparent).toContain('00-4bf92f3577b34da6a3ce929d0e0e4736');
      expect(response.headers.tracestate).toBe(headers[TRACE_STATE_HEADER]);
    });

    test('New trace context is propagated out of workflow', async () => {
      const response = await request(DBOS.getHTTPHandlersCallback()!).get('/hello');
      expect(response.statusCode).toBe(200);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      expect(response.body.message).toBe('hello joe');
      // traceId should be the same, spanId should be different (ID of the last operation's span)
      expect(response.headers.traceparent).not.toBe(null);
    });
  });
});
