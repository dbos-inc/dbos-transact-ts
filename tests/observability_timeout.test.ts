import { Client, PoolClient } from 'pg';
import { DBOS, DBOSClient, DBOSConfig } from '../src';
import { translateDbosConfig } from '../src/config';
import { DBOSExecutor } from '../src/dbos-executor';
import { DBOSQueryTimeoutError } from '../src/error';
import { DBOSJSON, DBOSSerializer } from '../src/serialization';
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
  function makeSysDb(
    observabilityQueryTimeoutMs?: number,
    poolSize: number = 2,
    serializer: DBOSSerializer = DBOSJSON,
  ): SystemDatabase {
    return new SystemDatabase(
      systemDatabaseUrl,
      new GlobalLogger(),
      serializer,
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
  function captureStatements(sysdb: SystemDatabase, statements: string[] = []): string[] {
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

  /** The capped transaction, reached the way the repo's other tests reach soft-private internals. */
  type Capped = { observabilityQuery<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> };
  const capped = (sysdb: SystemDatabase) => sysdb as unknown as Capped;

  const show = async (client: PoolClient, setting: string) =>
    (await client.query<Record<string, string>>(`SHOW ${setting}`)).rows[0][setting];

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

  /** Hold ACCESS EXCLUSIVE on workflow_status, so any read of it blocks until the cap cancels it. */
  async function whileWorkflowStatusIsLocked<T>(fn: () => Promise<T>): Promise<T> {
    const blocker = new Client(getClientConfig(systemDatabaseUrl));
    blocker.on('error', () => {});
    await blocker.connect();
    try {
      await blocker.query('BEGIN');
      await blocker.query('LOCK TABLE "dbos".workflow_status IN ACCESS EXCLUSIVE MODE');
      return await fn();
    } finally {
      await blocker.query('ROLLBACK').catch(() => {});
      await blocker.end().catch(() => {});
    }
  }

  test('the statement timeout cancels a blocked query', async () => {
    const sysdb = makeSysDb(300);
    try {
      const error = await whileWorkflowStatusIsLocked(() => sysdb.listWorkflows({}).catch((e: unknown) => e));
      expect(error).toBeInstanceOf(DBOSQueryTimeoutError);
      // No cause: dbRetry unwraps cause chains and would retry the underlying 57014 forever.
      expect((error as { cause?: unknown }).cause).toBeUndefined();
      // The cancelled query leaves its connection usable.
      await expect(sysdb.listWorkflows({})).resolves.toBeDefined();
    } finally {
      await sysdb.destroy();
    }
  });

  test('a timed-out query under @dbRetry rejects instead of retrying forever', async () => {
    const sysdb = makeSysDb(300);
    const start = new Date(0).toISOString();
    const end = new Date(Date.now() + 3_600_000).toISOString();
    try {
      const error = await whileWorkflowStatusIsLocked(() => sysdb.getMetrics(start, end).catch((e: unknown) => e));
      expect(error).toBeInstanceOf(DBOSQueryTimeoutError);
    } finally {
      await sysdb.destroy();
    }
  });

  test('a capped query runs under the cap, at read committed', async () => {
    const sysdb = makeSysDb();
    try {
      const settings = await capped(sysdb).observabilityQuery(async (client) => ({
        timeout: await show(client, 'statement_timeout'),
        isolation: await show(client, 'transaction_isolation'),
      }));
      expect(settings).toEqual({ timeout: '30s', isolation: 'read committed' });
    } finally {
      await sysdb.destroy();
    }
  });

  test('a non-timeout failure inside a capped query propagates unchanged', async () => {
    const sysdb = makeSysDb();
    try {
      const error = await capped(sysdb)
        .observabilityQuery((client) => client.query('SELECT * FROM "dbos".no_such_table'))
        .catch((e: unknown) => e);
      expect(error).not.toBeInstanceOf(DBOSQueryTimeoutError);
      expect((error as { code?: string }).code).toBe('42P01');
    } finally {
      await sysdb.destroy();
    }
  });

  test('a failed BEGIN does not return a poisoned client to the pool', async () => {
    // A one-connection pool, so the next query is guaranteed the same session.
    const sysdb = makeSysDb(undefined, 1);
    try {
      // Leave the pooled connection inside an aborted transaction, as a caller-supplied pool can.
      const client = await sysdb.pool.connect();
      await client.query('BEGIN');
      await client.query('SELECT * FROM "dbos".no_such_table').catch(() => {});
      client.release();

      // BEGIN fails with 25P02 on the inherited mess, but must leave the connection reusable.
      await expect(sysdb.listWorkflows({})).rejects.toThrow();
      await expect(sysdb.listWorkflows({})).resolves.toBeDefined();
    } finally {
      await sysdb.destroy();
    }
  });

  test('deserialization runs after the transaction commits', async () => {
    // node-postgres keeps the portal's snapshot alive until commit, so parsing inside would extend the hold the cap bounds.
    const log: string[] = [];
    const recording: DBOSSerializer = {
      name: () => 'recording',
      stringify: (value) => DBOSJSON.stringify(value),
      parse: (text) => {
        log.push('PARSE');
        return DBOSJSON.parse(text);
      },
    };
    const workflowID = 'observability-timeout-parse-order';
    const seed = new Client(getClientConfig(systemDatabaseUrl));
    await seed.connect();
    try {
      await seed.query(
        `INSERT INTO "dbos".workflow_status
           (workflow_uuid, status, name, authenticated_roles, created_at, updated_at, recovery_attempts)
         VALUES ($1, 'SUCCESS', 'parseOrderProbe', '[]', 1, 1, 1) ON CONFLICT DO NOTHING`,
        [workflowID],
      );
      // A null serialization routes the value through the handle's own serializer.
      await seed.query(
        `INSERT INTO "dbos".workflow_events (workflow_uuid, key, value, serialization) VALUES ($1, 'k', '1', NULL)
         ON CONFLICT DO NOTHING`,
        [workflowID],
      );

      const sysdb = makeSysDb(undefined, 2, recording);
      captureStatements(sysdb, log);
      try {
        await expect(sysdb.getAllEvents(workflowID)).resolves.toEqual({ k: 1 });
        expect(log).toContain('PARSE');
        expect(log.indexOf('COMMIT')).toBeLessThan(log.indexOf('PARSE'));
      } finally {
        await sysdb.destroy();
      }
    } finally {
      await seed.query(`DELETE FROM "dbos".workflow_status WHERE workflow_uuid = $1`, [workflowID]).catch(() => {});
      await seed.end();
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

  test('an out-of-range statement timeout is rejected', async () => {
    for (const bad of [2_147_483_648, 1e21]) {
      expect(() => translateDbosConfig({ ...config, observabilityQueryTimeoutMs: bad })).toThrow(
        'observabilityQueryTimeoutMs',
      );
      expect(() => makeSysDb(bad)).toThrow('observabilityQueryTimeoutMs');
    }
    // The largest value PostgreSQL accepts still is.
    const sysdb = makeSysDb(2_147_483_647);
    try {
      expect(sysdb.observabilityQueryTimeoutMs).toBe(2_147_483_647);
      await expect(sysdb.listWorkflows({})).resolves.toBeDefined();
    } finally {
      await sysdb.destroy();
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
      const workflows = await DBOS.listWorkflows({ workflowName: 'observabilityTimeoutWorkflow' });
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
