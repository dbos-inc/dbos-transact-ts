import {
  Operon,
  OperonConfig,
} from "src/";
import {
  generateOperonTestConfig,
  setupOperonTestDb,
} from './helpers';
import { Client, Pool } from 'pg';

describe('operon-init', () => {
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('successful init', async() => {
    const operon = new Operon(config);
    await operon.init();

    expect(operon.initialized).toBe(true);

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
});
