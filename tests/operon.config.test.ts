import {
  OperonConfig,
  DatabaseConfig,
} from 'src/';
import { PoolConfig } from 'pg';
import fs from 'fs';

jest.mock('fs');

describe('Operon config', () => {
  test('fs.stat fails', () => {
    const statMock = jest.spyOn(fs, 'stat').mockImplementation(() => {
      throw new Error('An error');
    });
    expect(() => new OperonConfig()).toThrow('calling fs.stat on operon-config.yaml: An error');
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
      connectionTimeoutMillis: 3,
      database: 'some database',
    };
    const mockConfigFile = {
      database: mockDatabaseConfig,
    };

    const readFileSyncMock = jest.spyOn(fs, 'readFileSync').mockReturnValue(JSON.stringify(mockConfigFile));

    const operonConfig: OperonConfig = new OperonConfig();

    // Test pool config options
    const poolConfig: PoolConfig = operonConfig.poolConfig;
    expect(poolConfig.host).toBe(mockDatabaseConfig.hostname);
    expect(poolConfig.port).toBe(mockDatabaseConfig.port);
    expect(poolConfig.user).toBe(mockDatabaseConfig.username);
    expect(poolConfig.password).toBe(process.env.PGPASSWORD);
    expect(poolConfig.connectionTimeoutMillis).toBe(mockDatabaseConfig.connectionTimeoutMillis);
    expect(poolConfig.database).toBe('some database');

    readFileSyncMock.mockRestore();
  });
});
