import { DBOSJSON } from '../src/serialization';
import { createSystemDatabase, SystemDatabase } from '../src/system_database';
import { GlobalLogger } from '../src/telemetry/logs';
import { dbRetryConfig } from '../src/utils';

// A 40001 as the pg driver raises it: matched by its SQLSTATE.
function serializationFailure(): Error {
  return Object.assign(new Error('restart transaction: TransactionRetryWithProtoRefreshError'), { code: '40001' });
}

describe('cockroach-serialization-retry', () => {
  let sysdb: SystemDatabase;
  let calls = 0;
  const saved = { ...dbRetryConfig };

  beforeEach(() => {
    dbRetryConfig.initialBackoffSec = 0;
    dbRetryConfig.maxBackoffSec = 0;
    calls = 0;
    // Never connects: every query goes to the fake pool below.
    sysdb = createSystemDatabase({
      systemDatabaseUrl: 'postgresql://postgres:dbos@localhost:5432/unused_cockroach_retry',
      logger: new GlobalLogger(),
      serializer: DBOSJSON,
      schemaName: 'dbos',
      useListenNotify: false,
    });
    Object.defineProperty(sysdb, 'pool', {
      value: {
        query: () => {
          calls++;
          if (calls === 1) return Promise.reject(serializationFailure());
          return Promise.resolve({ rows: [{ pk: 'p1' }] });
        },
      },
    });
  });

  afterEach(() => {
    Object.assign(dbRetryConfig, saved);
  });

  test('a decorated call reruns a lost serialization race on CockroachDB', async () => {
    jest.spyOn(sysdb, 'isCockroach').mockResolvedValue(true);
    await expect(sysdb.getQueuePartitions('q')).resolves.toEqual(['p1']);
    expect(calls).toBe(2);
  });

  test('on Postgres a 40001 is the caller to handle, as before', async () => {
    jest.spyOn(sysdb, 'isCockroach').mockResolvedValue(false);
    await expect(sysdb.getQueuePartitions('q')).rejects.toMatchObject({ code: '40001' });
    expect(calls).toBe(1);
  });
});
