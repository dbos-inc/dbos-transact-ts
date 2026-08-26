import { Client, PoolClient } from 'pg';
import { DBOS, DBOSClient, DBOSConfig } from '../src';
import { translateDbosConfig } from '../src/config';
import { DBOSExecutor } from '../src/dbos-executor';
import { DBOSQueryTimeoutError } from '../src/error';
import { DBOSJSON } from '../src/serialization';
import { SystemDatabase } from '../src/system_database';
import { GlobalLogger } from '../src/telemetry/logs';
import { getClientConfig } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

describe('observability-query-timeout', () => {
  let config: DBOSConfig;
  let systemDatabaseUrl: string;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    systemDatabaseUrl = translateDbosConfig(config).systemDatabaseUrl;
    await setUpDBOSTestSysDb(config);
  });

  /** A second handle on the test system database, so a test can pick its own timeout. */
  function makeSysDb(observabilityQueryTimeoutMs?: number, poolSize: number = 2): SystemDatabase {
    return new SystemDatabase(
      systemDatabaseUrl,
      new GlobalLogger(),
      DBOSJSON,
      poolSize,
      undefined,
      'dbos',
      false,
      undefined,
      undefined,
      undefined,
      observabilityQueryTimeoutMs,
    );
  }

  /** Every statement this handle issues, from the connect event on, so nothing predates the listener. */
  function captureStatements(sysdb: SystemDatabase): string[] {
    const statements: string[] = [];
    sysdb.pool.on('connect', (client: PoolClient) => {
      const query = client.query.bind(client);
      client.query = ((...args: Parameters<typeof query>) => {
        const first = args[0];
        statements.push(typeof first === 'string' ? first : String((first as { text: string }).text));
        return query(...args);
      }) as typeof client.query;
    });
    return statements;
  }

  const timeoutStatements = (statements: string[]) => statements.filter((s) => s.includes('statement_timeout'));

  const workflow = DBOS.registerWorkflow(async () => await DBOS.runStep(() => Promise.resolve(1), { name: 's' }), {
    name: 'observabilityTimeoutWorkflow',
  });

  test('observability queries carry a statement timeout', async () => {
    const sysdb = makeSysDb();
    const statements = captureStatements(sysdb);
    try {
      await sysdb.listWorkflows({});
      expect(timeoutStatements(statements)).toEqual(['SET LOCAL statement_timeout = 30000']);
    } finally {
      await sysdb.destroy();
    }
  });

  test('every observability query sets the timeout', async () => {
    const sysdb = makeSysDb();
    const statements = captureStatements(sysdb);
    const workflowID = 'no-such-workflow';
    const start = new Date(Date.now() - 3600_000).toISOString();
    const end = new Date(Date.now() + 3600_000).toISOString();

    const cases: [string, () => Promise<unknown>][] = [
      ['listWorkflows', () => sysdb.listWorkflows({})],
      ['getAllOperationResults', () => sysdb.getAllOperationResults(workflowID)],
      ['getWorkflowAggregates', () => sysdb.getWorkflowAggregates({ groupByStatus: true, selectCount: true })],
      ['getStepAggregates', () => sysdb.getStepAggregates({ groupByFunctionName: true, selectCount: true })],
      ['getMetrics', () => sysdb.getMetrics(start, end)],
      ['getAllEvents', () => sysdb.getAllEvents(workflowID)],
      ['getAllNotifications', () => sysdb.getAllNotifications(workflowID)],
      ['getAllStreamEntries', () => sysdb.getAllStreamEntries(workflowID)],
      ['listApplicationVersions', () => sysdb.listApplicationVersions()],
    ];

    try {
      for (const [name, call] of cases) {
        statements.length = 0;
        await call();
        expect([name, timeoutStatements(statements)]).toEqual([name, ['SET LOCAL statement_timeout = 30000']]);
      }
    } finally {
      await sysdb.destroy();
    }
  });

  test('an ID-keyed workflow lookup is not capped', async () => {
    const sysdb = makeSysDb();
    const statements = captureStatements(sysdb);
    try {
      await sysdb.listWorkflows({ workflowIDs: ['no-such-workflow'] });
      await sysdb.getWorkflowStatus('no-such-workflow');
      expect(timeoutStatements(statements)).toEqual([]);
    } finally {
      await sysdb.destroy();
    }
  });

  test('the statement timeout does not outlive its transaction', async () => {
    // A one-connection pool, so the next query is guaranteed the same session.
    const sysdb = makeSysDb(undefined, 1);
    try {
      await sysdb.listWorkflows({});
      const { rows } = await sysdb.pool.query<{ statement_timeout: string }>('SHOW statement_timeout');
      expect(rows[0].statement_timeout).toBe('0');
    } finally {
      await sysdb.destroy();
    }
  });

  test('the statement timeout cancels a blocked query', async () => {
    const sysdb = makeSysDb(300);
    const blocker = new Client(getClientConfig(systemDatabaseUrl));
    await blocker.connect();
    try {
      await blocker.query('BEGIN');
      await blocker.query('LOCK TABLE "dbos".workflow_status IN ACCESS EXCLUSIVE MODE');
      await expect(sysdb.listWorkflows({})).rejects.toThrow(DBOSQueryTimeoutError);
    } finally {
      await blocker.query('ROLLBACK');
      await blocker.end();
    }
    try {
      // The cancelled query leaves its connection usable.
      await expect(sysdb.listWorkflows({})).resolves.toEqual([]);
    } finally {
      await sysdb.destroy();
    }
  });

  test('the statement timeout can be disabled', async () => {
    const sysdb = makeSysDb(0);
    const statements = captureStatements(sysdb);
    try {
      expect(sysdb.observabilityQueryTimeoutMs).toBeUndefined();
      await sysdb.listWorkflows({});
      expect(timeoutStatements(statements)).toEqual([]);
    } finally {
      await sysdb.destroy();
    }
  });

  test('a sub-millisecond statement timeout still caps', async () => {
    // Rounding down to 0 would hand back no timeout at all, the loosest cap rather than the tightest.
    const sysdb = makeSysDb(0.4);
    try {
      expect(sysdb.observabilityQueryTimeoutMs).toBe(1);
    } finally {
      await sysdb.destroy();
    }
  });

  test('a non-finite statement timeout is rejected', () => {
    for (const bad of [NaN, Infinity]) {
      expect(() => translateDbosConfig({ ...config, observabilityQueryTimeoutMs: bad })).toThrow(
        'observabilityQueryTimeoutMs',
      );
      expect(() => makeSysDb(bad)).toThrow('observabilityQueryTimeoutMs');
    }
  });

  test('a configured statement timeout reaches the system database', async () => {
    DBOS.setConfig({ ...config, observabilityQueryTimeoutMs: 5000 });
    await DBOS.launch();
    try {
      const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
      expect(sysdb.observabilityQueryTimeoutMs).toBe(5000);

      // The timeout must not disturb a query that finishes inside it.
      await expect(workflow()).resolves.toBe(1);
      const workflows = await DBOS.listWorkflows({});
      expect(workflows.length).toBe(1);
      await expect(DBOS.listWorkflowSteps(workflows[0].workflowID)).resolves.toHaveLength(1);
    } finally {
      await DBOS.shutdown();
    }
  });

  test('a client statement timeout reaches the system database', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl, observabilityQueryTimeoutMs: 5000 });
    try {
      const sysdb = (client as unknown as { systemDatabase: SystemDatabase }).systemDatabase;
      expect(sysdb.observabilityQueryTimeoutMs).toBe(5000);
      await expect(client.listWorkflows({})).resolves.toBeDefined();
    } finally {
      await client.destroy();
    }
  });
});
