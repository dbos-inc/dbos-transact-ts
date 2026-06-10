import { randomUUID } from 'node:crypto';
import { ContextualMetadata, DBOS, DBOSClient, DLogger, StackTrace } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

type RecordedEntry = {
  level: 'info' | 'debug' | 'warn' | 'error';
  entry: unknown;
  metadata?: ContextualMetadata & StackTrace;
};

class RecorderLogger implements DLogger {
  readonly entries: RecordedEntry[] = [];

  info(logEntry: unknown, metadata?: ContextualMetadata): void {
    this.entries.push({ level: 'info', entry: logEntry, metadata });
  }
  debug(logEntry: unknown, metadata?: ContextualMetadata): void {
    this.entries.push({ level: 'debug', entry: logEntry, metadata });
  }
  warn(logEntry: unknown, metadata?: ContextualMetadata): void {
    this.entries.push({ level: 'warn', entry: logEntry, metadata });
  }
  error(inputError: unknown, metadata?: ContextualMetadata & StackTrace): void {
    this.entries.push({ level: 'error', entry: inputError, metadata });
  }

  find(level: RecordedEntry['level'], entry: unknown): RecordedEntry | undefined {
    return this.entries.find((e) => e.level === level && e.entry === entry);
  }
}

class LoggingWF {
  @DBOS.step()
  static async loggingStep() {
    DBOS.logger.info('marker-in-step');
    return Promise.resolve(1);
  }

  @DBOS.workflow()
  static async loggingWorkflow() {
    DBOS.logger.info('marker-in-wf');
    return await LoggingWF.loggingStep();
  }
}

describe('custom-logger', () => {
  let recorder: RecorderLogger;

  beforeAll(async () => {
    await setUpDBOSTestSysDb(generateDBOSTestConfig());
  });

  beforeEach(() => {
    recorder = new RecorderLogger();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('workflow and step logs reach the custom logger with context metadata', async () => {
    const consoleSpies = [
      jest.spyOn(console, 'log'),
      jest.spyOn(console, 'debug'),
      jest.spyOn(console, 'warn'),
      jest.spyOn(console, 'error'),
    ];
    try {
      DBOS.setConfig({ ...generateDBOSTestConfig(), logger: recorder, tracingEnabled: true });
      await DBOS.launch();

      // Internal lifecycle logs must also flow through the custom logger.
      expect(recorder.find('info', 'DBOS launched!')).toBeDefined();

      const wfid = randomUUID();
      await DBOS.withNextWorkflowID(wfid, async () => {
        await LoggingWF.loggingWorkflow();
      });

      const wfEntry = recorder.find('info', 'marker-in-wf');
      expect(wfEntry).toBeDefined();
      expect(wfEntry?.metadata?.span?.attributes?.operationUUID).toBe(wfid);
      expect(wfEntry?.metadata?.span?.attributes?.operationType).toBe('workflow');
      expect(wfEntry?.metadata?.span?.attributes?.operationName).toBe('loggingWorkflow');

      const stepEntry = recorder.find('info', 'marker-in-step');
      expect(stepEntry).toBeDefined();
      expect(stepEntry?.metadata?.span?.attributes?.operationUUID).toBe(wfid);
      expect(stepEntry?.metadata?.span?.attributes?.operationType).toBe('step');
      expect(stepEntry?.metadata?.span?.attributes?.operationName).toBe('loggingStep');

      // Both user markers and internal logs must reach only the custom logger,
      // not a built-in console sink.
      for (const spy of consoleSpies) {
        for (const call of spy.mock.calls) {
          expect(String(call[0])).not.toContain('marker-in-wf');
          expect(String(call[0])).not.toContain('marker-in-step');
          expect(String(call[0])).not.toContain('DBOS launched!');
        }
      }
    } finally {
      consoleSpies.forEach((s) => s.mockRestore());
    }
  });

  test('error entries are normalized to message plus stack metadata', async () => {
    DBOS.setConfig({ ...generateDBOSTestConfig(), logger: recorder });
    await DBOS.launch();

    DBOS.logger.error(new Error('boom'));
    const entry = recorder.find('error', 'boom');
    expect(entry).toBeDefined();
    expect(typeof entry?.metadata?.stack).toBe('string');
  });

  test('non-string entries are stringified before delegation', async () => {
    DBOS.setConfig({ ...generateDBOSTestConfig(), logger: recorder });
    await DBOS.launch();

    DBOS.logger.info({ a: 1 });
    const entry = recorder.entries.find(
      (e) => e.level === 'info' && typeof e.entry === 'string' && e.entry.includes('"a":1'),
    );
    expect(entry).toBeDefined();
  });

  test('logLevel does not filter entries delegated to the custom logger', async () => {
    DBOS.setConfig({ ...generateDBOSTestConfig(), logger: recorder, logLevel: 'error' });
    await DBOS.launch();

    DBOS.logger.debug('debug-marker');
    expect(recorder.find('debug', 'debug-marker')).toBeDefined();
  });

  test('DBOS.logger honors the configured logger before launch', () => {
    DBOS.setConfig({ ...generateDBOSTestConfig(), logger: recorder });
    DBOS.logger.info('pre-launch-marker');
    expect(recorder.find('info', 'pre-launch-marker')).toBeDefined();
  });

  test('DBOS.logger honors the configured logLevel before launch', () => {
    const debugSpy = jest.spyOn(console, 'debug').mockImplementation(() => {});
    try {
      DBOS.setConfig({ ...generateDBOSTestConfig(), logLevel: 'error' });
      DBOS.logger.debug('pre-launch-debug-marker');
      expect(debugSpy.mock.calls.some((c) => String(c[0]).includes('pre-launch-debug-marker'))).toBe(false);

      DBOS.setConfig({ ...generateDBOSTestConfig(), logLevel: 'debug' });
      DBOS.logger.debug('pre-launch-debug-marker');
      expect(debugSpy.mock.calls.some((c) => String(c[0]).includes('pre-launch-debug-marker'))).toBe(true);
    } finally {
      debugSpy.mockRestore();
    }
  });

  test('custom logger takes over when OTLP is enabled', async () => {
    const logSpy = jest.spyOn(console, 'log');
    try {
      DBOS.setConfig({ ...generateDBOSTestConfig(), logger: recorder, enableOTLP: true });
      await DBOS.launch();

      DBOS.logger.info('otlp-marker');
      expect(recorder.find('info', 'otlp-marker')).toBeDefined();
      for (const call of logSpy.mock.calls) {
        expect(String(call[0])).not.toContain('otlp-marker');
      }
    } finally {
      logSpy.mockRestore();
    }
  });

  test('DBOSClient logs through an injected logger', async () => {
    const config = generateDBOSTestConfig();
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl!, logger: recorder });
    try {
      (client as unknown as { logger: DLogger }).logger.info('client-marker');
      expect(recorder.find('info', 'client-marker')).toBeDefined();
    } finally {
      await client.destroy();
    }
  });
});
