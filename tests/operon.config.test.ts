import {
  OperonConfig,
  DatabaseConfig,
} from 'src/';
import { PoolConfig } from 'pg';
import fs from 'fs';

jest.mock('fs');

describe('Operon config', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('fs.stat fails', () => {
    jest.spyOn(fs, 'stat').mockImplementation(() => {
      throw new Error('An error');
    });
    expect(() => new OperonConfig()).toThrow('calling fs.stat on operon-config.yaml: An error');
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
      schemaFile: 'some schema file',
    };
    const mockConfigFile = {
      database: mockDatabaseConfig,
    };

    jest.spyOn(fs, 'readFileSync').mockReturnValueOnce(JSON.stringify(mockConfigFile));
    jest.spyOn(fs, 'readFileSync').mockReturnValueOnce("SQL STATEMENTS");

    const operonConfig: OperonConfig = new OperonConfig();

    // Test pool config options
    const poolConfig: PoolConfig = operonConfig.poolConfig;
    expect(poolConfig.host).toBe(mockDatabaseConfig.hostname);
    expect(poolConfig.port).toBe(mockDatabaseConfig.port);
    expect(poolConfig.user).toBe(mockDatabaseConfig.username);
    expect(poolConfig.password).toBe(process.env.PGPASSWORD);
    expect(poolConfig.connectionTimeoutMillis).toBe(mockDatabaseConfig.connectionTimeoutMillis);
    // expect(poolConfig.database).toBe('operon');
    expect(poolConfig.database).toBe('postgres');

    // Test schema file has been set
    expect(operonConfig.operonDbSchema).toBe('SQL STATEMENTS');
  });

  test('config file is empty', () => {
    const mockConfigFile = '';
    jest.spyOn(fs, 'readFileSync').mockReturnValueOnce(JSON.stringify(mockConfigFile));
    expect(() => new OperonConfig()).toThrow('Operon configuration operon-config.yaml is empty');
  });

  test('config file is missing database config', () => {
    const mockConfigFile = {};
    jest.spyOn(fs, 'readFileSync').mockReturnValueOnce(JSON.stringify(mockConfigFile));
    expect(() => new OperonConfig()).toThrow(
      'Operon configuration operon-config.yaml does not contain database config'
    );
  });
});
