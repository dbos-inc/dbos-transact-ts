import {
  Operon,
  OperonConfig,
  OperonInitializationError,
} from "src/";
import {
  generateOperonTestConfig,
  teardownOperonTestDb,
} from './helpers';
import * as utils from  '../src/utils';
import { Client, Pool } from 'pg';

describe('operon-init', () => {
  let config: OperonConfig;

  beforeAll(() => {
    config = generateOperonTestConfig();
  });

  afterEach(() => {
    jest.restoreAllMocks();
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

    // Test notification client is set and has registered a listener
    expect(operon.pgNotificationsClient).toBeDefined();
    expect(operon.pgNotificationsClient).toBeInstanceOf(Client);
    const listenersQueryResult = await operon.pgNotificationsClient.query(`SELECT * from pg_listening_channels()`);
    expect(listenersQueryResult.rows).toHaveLength(1);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(listenersQueryResult.rows[0].pg_listening_channels).toBe('operon__notificationschannel');

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
    await teardownOperonTestDb(config);
  });

  test('init can only be called once', async() => {
    const operon = new Operon(config);
    const loadOperonDatabaseSpy = jest.spyOn(operon, 'loadOperonDatabase');
    const listenForNotificationsSpy = jest.spyOn(operon, 'listenForNotifications');
    await operon.init();
    await operon.init();
    expect(loadOperonDatabaseSpy).toHaveBeenCalledTimes(1);
    expect(listenForNotificationsSpy).toHaveBeenCalledTimes(1);
    await operon.destroy();
    await teardownOperonTestDb(config);
  });

  test('fails to read schema file', async () => {
    const operon = new Operon(config);
    jest.spyOn(utils, 'readFileSync').mockImplementation(() => { throw(new Error('some error')); });
    await expect(operon.loadOperonDatabase()).rejects.toThrow('some error');
  });

  test('schema file is empty', async () => {
    // We need a new DB for this test so we can enter the schema loading path
    const cfg: OperonConfig = generateOperonTestConfig();
    const operon = new Operon(cfg);
    jest.spyOn(utils, 'readFileSync').mockReturnValue('');
    await expect(operon.init()).rejects.toThrow(OperonInitializationError);
    await operon.destroy();
  });

  // test fails to create listener
  test('Failing to create listener throws', async() => {
    const operon = new Operon();
    jest.spyOn(operon, 'listenForNotifications').mockImplementation(() => {
      throw new Error('mock error');
    });
    await expect(operon.init()).rejects.toThrow(OperonInitializationError);
  });
});
