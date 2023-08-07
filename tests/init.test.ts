import {
  Operon,
  OperonConfig,
} from "src/";
import {
  generateOperonTestConfig,
  teardownOperonTestDb,
} from './helpers';
import { Client, Pool } from 'pg';

describe('operon-init', () => {
  let config: OperonConfig;

  beforeAll(() => {
    config = generateOperonTestConfig();
  });

  afterEach(async () => {
    jest.restoreAllMocks();
    await teardownOperonTestDb(config);
  });

  test('successful init', async() => {
    const operon = new Operon(config);
    await operon.init();

    expect(operon.initialized).toBe(true);

    // Check pgSystemClient has been shutdown
    expect(operon.pgSystemClient).toBeDefined();
    expect(operon.pgSystemClient).toBeInstanceOf(Client);
    await expect(
      operon.pgSystemClient.query(`SELECT FROM pg_database WHERE datname = 'postgres';`)
    ).rejects.toThrow('Client was closed and is not queryable');

    // Test "global" pool
    expect(operon.pool).toBeDefined();
    expect(operon.pool).toBeInstanceOf(Pool);
    // Can connect and retrieve a client
    const poolClient = await operon.pool.connect();
    expect(poolClient).toBeDefined();
    expect(poolClient).toBeInstanceOf(Client);
    // Can use the client to query
    const queryResult = await poolClient.query(`select current_user from current_user`);
    expect(queryResult.rows).toHaveLength(1);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(queryResult.rows[0].current_user).toBeDefined();
    poolClient.release();

    await operon.destroy();
    // TODO check that resources have been released. The client objects hold that information but it is not exposed.
  });

  test('init can only be called once', async() => {
    const operon = new Operon(config);
    const loadOperonDatabaseSpy = jest.spyOn(operon, 'loadOperonDatabase');
    await operon.init();
    await operon.init();
    expect(loadOperonDatabaseSpy).toHaveBeenCalledTimes(1);
    await operon.destroy();
  });

  test('Attempt to inject SQL in the database name fails', async() => {
    const newConfig: OperonConfig = generateOperonTestConfig();
    newConfig.poolConfig.database = `${newConfig.poolConfig.database}; DROP SCHEMA public;`;
    const operon = new Operon(newConfig);
    await expect(operon.init()).rejects.toThrow(`invalid DB name: ${newConfig.poolConfig.database}`);
    await operon.destroy();
  });
});
