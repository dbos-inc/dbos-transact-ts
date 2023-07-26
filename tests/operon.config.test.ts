import {
  Operon,
  OperonConfigFile,
  DatabaseConfig,
} from 'src/';
import { Pool } from 'pg';
import fs from 'fs';

jest.mock('fs');

describe('Operon config', () => {
  test('fs.stat fails', () => {
    const statMock = jest.spyOn(fs, 'stat').mockImplementation(() => {
      throw new Error('An error');
    });
    expect(() => new Operon()).toThrow('calling fs.stat on operon-config.yaml: An error');
    statMock.mockRestore();
  });

  // TODO
  test('System error while checking on config file', () => {
  });

  // TODO
  test('Config file is not a valid file', () => {
  });

  test('Config is valid and is parsed as expected', () => {
    const mockDatabaseConfig: DatabaseConfig = {
      hostname: 'some host',
      port: 1111,
      username: 'some test user',
      password: 'some test password',
      connectionTimeoutMillis: 3,
    };
    const mockConfigFile: OperonConfigFile = {
      database: mockDatabaseConfig,
    };

    const readFileSyncMock = jest.spyOn(fs, 'readFileSync').mockReturnValue(JSON.stringify(mockConfigFile));

    const operon = new Operon();
    const pool: Pool = operon.pool ;
    // Pool is subclassed as BoundPool by pg-node.
    // But pg only exports the `pool` type
    // So we do some terrible things to retrieve the `options` property
    expect(pool.hasOwnProperty('options')).toBe(true); // eslint-disable-line no-prototype-builtins
    /* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access */
    const options = Object.getOwnPropertyDescriptors(pool)['options'].value;
    expect(options.host).toBe(mockDatabaseConfig.hostname);
    expect(options.port).toBe(mockDatabaseConfig.port);
    expect(options.user).toBe(mockDatabaseConfig.username);
    expect(options.password).toBe(mockDatabaseConfig.password);
    expect(options.connectionTimeoutMillis).toBe(mockDatabaseConfig.connectionTimeoutMillis);
    expect(options.database).toBe('postgres');
    expect(options.max).toBe(10);

    readFileSyncMock.mockRestore();
  });
});
