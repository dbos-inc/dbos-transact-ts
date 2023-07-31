import {
  Operon,
  OperonConfig,
  OperonInitializationError,
} from 'src/';
import * as utils from  '../src/utils';
import { PoolConfig } from 'pg';

describe('operon-config', () => {
  const mockOperonConfigYamlString = `
      database:
        hostname: 'some host'
        port: 1234
        username: 'some user'
        connectionTimeoutMillis: 3000
        schemaFile: 'schema.sql'
        database: 'some DB'
      `;

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('Config is valid and is parsed as expected', () => {
    jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockOperonConfigYamlString);
    jest.spyOn(utils, 'readFileSync').mockReturnValueOnce("SQL STATEMENTS");

    const operon: Operon = new Operon();
    const operonConfig: OperonConfig = operon.config;

    // Test pool config options
    const poolConfig: PoolConfig = operonConfig.poolConfig;
    expect(poolConfig.host).toBe('some host');
    expect(poolConfig.port).toBe(1234);
    expect(poolConfig.user).toBe('some user');
    expect(poolConfig.password).toBe(process.env.PGPASSWORD);
    expect(poolConfig.connectionTimeoutMillis).toBe(3000);
    expect(poolConfig.database).toBe('some DB');

    // Test schema file has been set
    expect(operonConfig.operonSystemDbSchemaFile).toBe('schema.sql');
  });

  test('fails to read config file', () => {
    jest.spyOn(utils, 'readFileSync').mockImplementation(() => { throw(new Error('some error')); });
    expect(() => new Operon()).toThrow(OperonInitializationError);
  });

  test('config file is empty', () => {
    const mockConfigFile = '';
    jest.spyOn(utils, 'readFileSync').mockReturnValue(JSON.stringify(mockConfigFile));
    expect(() => new Operon()).toThrow(OperonInitializationError);
  });

  test('config file is missing database config', () => {
    const mockConfigFile = {someOtherConfig: 'some other config'};
    jest.spyOn(utils, 'readFileSync').mockReturnValue(JSON.stringify(mockConfigFile));
    expect(() => new Operon()).toThrow(OperonInitializationError);
  });

  test('config file is missing schema file', () => {
    const mockOperonConfigYamlString = `
      database:
        hostname: 'some host'
        port: 1234
        username: 'some user'
        connectionTimeoutMillis: 3000
        database: 'some DB'
      `;
    jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockOperonConfigYamlString);
    expect(() => new Operon()).toThrow(OperonInitializationError);
  });
});
