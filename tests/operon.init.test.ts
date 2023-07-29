import {
  Operon,
} from "src/";
import { Client, Pool } from 'pg';

describe('operon-init', () => {
  test('successful init', async() => {
    const operon = new Operon(); // TODO have this test pass a custom config with a tmp database schema
    await operon.init();

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
  });

  // test: two inits in a row do not attempt to re-create the database


  // test: fails to create database

  // test: fails to load operon schema

  // test fails to create listener

});
