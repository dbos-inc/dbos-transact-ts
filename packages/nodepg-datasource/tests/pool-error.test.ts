import { DBOS } from '@dbos-inc/dbos-sdk';
import { Pool } from 'pg';
import { NodePostgresDataSource } from '..';

describe('NodePostgresDataSource internal pool', () => {
  test('attaches an error listener on the pool it creates', async () => {
    const spy = jest
      .spyOn(Pool.prototype, 'connect')
      .mockImplementation(() => Promise.reject(new Error('stop initialize')));
    new NodePostgresDataSource('pool-error-test', { host: 'nowhere' });
    DBOS.setConfig({ name: 'nodepg-pool-error-test' });
    try {
      await DBOS.launch().catch(() => undefined);
      const pool = spy.mock.contexts[0] as Pool | undefined;
      expect(pool).toBeDefined();
      expect(() => pool!.emit('error', new Error('boom'))).not.toThrow();
    } finally {
      await DBOS.shutdown().catch(() => undefined);
      spy.mockRestore();
    }
  });
});
