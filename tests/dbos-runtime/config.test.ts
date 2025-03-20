/* eslint-disable */

import fs from 'fs';
import * as utils from '../../src/utils';
import { UserDatabaseName } from '../../src/user_database';
import { PoolConfig } from 'pg';
import { parseConfigFile, parseDbString, translatePublicDBOSconfig } from '../../src/dbos-runtime/config';
import { DBOSRuntimeConfig, defaultEntryPoint } from '../../src/dbos-runtime/runtime';
import { DBOSConfigKeyTypeError, DBOSInitializationError } from '../../src/error';
import { DBOSExecutor, DBOSConfig } from '../../src/dbos-executor';
import { WorkflowContextImpl } from '../../src/workflow';
import { get } from 'lodash';
import { db_wizard } from '../../src/dbos-runtime/db_wizard';

describe('dbos-config', () => {
  const mockCLIOptions = { port: NaN, loglevel: 'info' };
  const mockDBOSConfigYamlString = `
      name: 'some app'
      language: 'node'
      database:
        hostname: 'some host'
        port: 1234
        username: 'some user'
        password: \${PGPASSWORD}
        app_db_name: 'some_db'
        ssl: false
      application:
        payments_url: 'http://somedomain.com/payment'
        foo: \${FOO}
        bar: \${BAR}
        nested:
            baz: \${BAZ}
            a:
              - 1
              - 2
              - b:
                  c: \${C}
    `;

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('Configuration parsing', () => {
    // reset environment variables for each test as per https://stackoverflow.com/a/48042799
    const OLD_ENV = process.env;

    beforeEach(() => {
      jest.resetModules(); // Most important - it clears the cache
      process.env = { ...OLD_ENV }; // Make a copy
    });

    afterAll(() => {
      process.env = OLD_ENV; // Restore old environment
    });

    test('Config is valid and is parsed as expected', () => {
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);

      const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);

      // Test pool config options
      const poolConfig: PoolConfig = dbosConfig.poolConfig!;
      expect(poolConfig.host).toBe('some host');
      expect(poolConfig.port).toBe(1234);
      expect(poolConfig.user).toBe('some user');
      expect(poolConfig.password).toBe(process.env.PGPASSWORD);
      expect(poolConfig.connectionTimeoutMillis).toBe(3000);
      expect(poolConfig.database).toBe('some_db');
      expect(poolConfig.ssl).toBe(false);

      expect(dbosConfig.userDbclient).toBe(UserDatabaseName.KNEX);

      // Application config
      const applicationConfig: object = dbosConfig.application || {};
      expect(get(applicationConfig, 'payments_url')).toBe('http://somedomain.com/payment');
      expect(get(applicationConfig, 'foo')).toBe(process.env.FOO);
      expect(get(applicationConfig, 'bar')).toBe(process.env.BAR);
      expect(get(applicationConfig, 'nested.baz')).toBe(process.env.BAZ);
      expect(get(applicationConfig, 'nested.a')).toBeInstanceOf(Array);
      expect(get(applicationConfig, 'nested.a')).toHaveLength(3);
      expect(get(applicationConfig, 'nested.a[2].b.c')).toBe(process.env.C);

      // local runtime config
      expect(runtimeConfig).toBeDefined();
      expect(runtimeConfig.entrypoints).toBeDefined();
      expect(runtimeConfig.entrypoints).toBeInstanceOf(Array);
      expect(runtimeConfig.entrypoints).toHaveLength(1);
      expect(runtimeConfig.entrypoints[0]).toBe(defaultEntryPoint);
    });

    test('runtime config correctly parses entrypoints', () => {
      const mockDBOSConfigWithEntryPoints =
        mockDBOSConfigYamlString +
        `\n
      runtimeConfig:
        port: 1234
        entrypoints:
          - a
          - b
          - b
        admin_port: 2345
      `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigWithEntryPoints);

      const [_, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);

      expect(runtimeConfig).toBeDefined();
      expect(runtimeConfig?.port).toBe(1234);
      expect(runtimeConfig?.admin_port).toBe(2345);
      expect(runtimeConfig.entrypoints).toBeDefined();
      expect(runtimeConfig.entrypoints).toBeInstanceOf(Array);
      expect(runtimeConfig.entrypoints).toHaveLength(2);
      expect(runtimeConfig.entrypoints[0]).toBe('a');
      expect(runtimeConfig.entrypoints[1]).toBe('b');
    });

    test('fails to read config file', () => {
      jest.spyOn(utils, 'readFileSync').mockImplementation(() => {
        throw new DBOSInitializationError('some error');
      });
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test('config file is empty', () => {
      const mockConfigFile = '';
      jest.spyOn(utils, 'readFileSync').mockReturnValue(JSON.stringify(mockConfigFile));
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test('config file has unexpected fields', () => {
      const mockConfigFile = { someOtherConfig: 'some other config' };
      jest.spyOn(utils, 'readFileSync').mockReturnValue(JSON.stringify(mockConfigFile));
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test('config file loads default without database section', () => {
      const localMockDBOSConfigYamlString = `
        name: some-app
      `;
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(localMockDBOSConfigYamlString);
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce('SQL STATEMENTS');
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig!.host).toEqual('localhost');
      expect(dbosConfig.poolConfig!.port).toEqual(5432);
      expect(dbosConfig.poolConfig!.user).toEqual('postgres');
      expect(dbosConfig.poolConfig!.password).toEqual(process.env.PGPASSWORD);
      expect(dbosConfig.poolConfig!.database).toEqual('some_app');
    });

    test('config file loads mixed params', () => {
      const localMockDBOSConfigYamlString = `
        name: some-app
        database:
          hostname: 'some host'
      `;
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(localMockDBOSConfigYamlString);
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce('SQL STATEMENTS');
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig!.host).toEqual('some host');
      expect(dbosConfig.poolConfig!.port).toEqual(5432);
      expect(dbosConfig.poolConfig!.user).toEqual('postgres');
      expect(dbosConfig.poolConfig!.password).toEqual(process.env.PGPASSWORD);
      expect(dbosConfig.poolConfig!.database).toEqual('some_app');
    });

    test('using dbconnection file', () => {
      const localMockDBOSConfigYamlString = `
        name: some-app
      `;
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(localMockDBOSConfigYamlString);
      const mockDatabaseConnectionFile = `
        {"hostname": "example.com", "port": 2345, "username": "example", "password": "password", "local_suffix": true}
      `;
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockDatabaseConnectionFile);
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce('SQL STATEMENTS');
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig!.host).toEqual('example.com');
      expect(dbosConfig.poolConfig!.port).toEqual(2345);
      expect(dbosConfig.poolConfig!.user).toEqual('example');
      expect(dbosConfig.poolConfig!.password).toEqual('password');
      expect(dbosConfig.poolConfig!.database).toEqual('some_app_local');
    });

    test('config file specifies the wrong language', () => {
      const localMockDBOSConfigYamlString = `
      language: 'python'
      database:
        hostname: 'some host'
        port: 1234
        username: 'some user'
        password: \${PGPASSWORD}
        app_db_name: 'some_db'
        ssl: false
    `;
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(localMockDBOSConfigYamlString);
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce('SQL STATEMENTS');
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test('Config pool settings can be overridden by environment variables', () => {
      const mockDBOSConfigYamlString = `
      name: 'some app'
      language: 'node'
      database:
        hostname: 'some host'
        port: 1234
        username: 'some user'
        password: 'some password'
        app_db_name: 'some_db'
        local_suffix: true
        ssl: false`;

      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);

      process.env.DBOS_DBHOST = 'DBHOST_OVERRIDE';
      process.env.DBOS_DBPORT = '99999';
      process.env.DBOS_DBUSER = 'DBUSER_OVERRIDE';
      process.env.DBOS_DBPASSWORD = 'DBPASSWORD_OVERRIDE';
      process.env.DBOS_DBLOCALSUFFIX = 'false';

      const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);

      const poolConfig: PoolConfig = dbosConfig.poolConfig!;
      expect(poolConfig.host).toBe('DBHOST_OVERRIDE');
      expect(poolConfig.port).toBe(99999);
      expect(poolConfig.user).toBe('DBUSER_OVERRIDE');
      expect(poolConfig.password).toBe('DBPASSWORD_OVERRIDE');
      expect(poolConfig.database).toBe('some_db');
    });

    test('DB wizard will not start with database configured', async () => {
      const mockDBOSConfigYamlString = `
      name: 'some-app'
      language: 'node'
      database:
        hostname: 'localhost'
        port: 5432
        password: 'somerandom'`;

      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);

      const [dbosConfig, _]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);

      const poolConfig: PoolConfig = dbosConfig.poolConfig!;
      expect(poolConfig.host).toBe('localhost');
      expect(poolConfig.port).toBe(5432);
      await expect(db_wizard(poolConfig)).rejects.toThrow(DBOSInitializationError);
    });
  });

  describe('context getConfig()', () => {
    beforeEach(() => {
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);
    });

    afterEach(() => {
      jest.restoreAllMocks();
    });

    test('getConfig returns the expected values', async () => {
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const dbosExec = new DBOSExecutor(dbosConfig);
      const ctx: WorkflowContextImpl = new WorkflowContextImpl(
        dbosExec,
        undefined,
        'testUUID',
        {},
        'testContext',
        true,
        undefined,
        undefined,
      );
      // Config key exists
      expect(ctx.getConfig('payments_url')).toBe('http://somedomain.com/payment');
      // Config key does not exist, no default value
      expect(ctx.getConfig('no_key')).toBeUndefined();
      // Config key does not exist, default value
      expect(ctx.getConfig('no_key', 'default')).toBe('default');
      // We didn't init, so do some manual cleanup only
      clearInterval(dbosExec.flushBufferID);
      await dbosExec.telemetryCollector.destroy();
    });

    test('getConfig returns the default value when no application config is provided', async () => {
      const localMockDBOSConfigYamlString = `
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
          app_db_name: 'some_db'
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const dbosExec = new DBOSExecutor(dbosConfig);
      const ctx: WorkflowContextImpl = new WorkflowContextImpl(
        dbosExec,
        undefined,
        'testUUID',
        {},
        'testContext',
        true,
        undefined,
        undefined,
      );
      expect(ctx.getConfig<string>('payments_url', 'default')).toBe('default');
      // We didn't init, so do some manual cleanup only
      clearInterval(dbosExec.flushBufferID);
      await dbosExec.telemetryCollector.destroy();
    });

    test('environment variables are set correctly', async () => {
      const localMockDBOSConfigYamlString = `
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
          app_db_name: 'some_db'
        env:
          FOOFOO: barbar
          RANDENV: \${SOMERANDOMENV}
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const dbosExec = new DBOSExecutor(dbosConfig);
      expect(process.env.FOOFOO).toBe('barbar');
      expect(process.env.RANDENV).toBe(''); // Empty string
      // We didn't init, so do some manual cleanup only
      clearInterval(dbosExec.flushBufferID);
      await dbosExec.telemetryCollector.destroy();
    });

    test('ssl true enables ssl', async () => {
      const localMockDBOSConfigYamlString = `
        database:
          hostname: 'localhost'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
          app_db_name: 'some_db'
          ssl: true
        env:
          FOOFOO: barbar
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig!.ssl).toEqual({ rejectUnauthorized: false });
    });

    test('config works without app_db_name', async () => {
      const localMockDBOSConfigYamlString = `
        name: some-app
        database:
          hostname: 'localhost'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const poolConfig = dbosConfig.poolConfig;
      expect(poolConfig!.database).toBe('some_app');
    });

    test('local_suffix works', async () => {
      const localMockDBOSConfigYamlString = `
        database:
          hostname: 'remote.com'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
          app_db_name: 'some_db'
          local_suffix: true
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const poolConfig = dbosConfig.poolConfig;
      expect(poolConfig!.host).toBe('remote.com');
      expect(poolConfig!.port).toBe(1234);
      expect(poolConfig!.user).toBe('some user');
      expect(poolConfig!.password).toBe(process.env.PGPASSWORD);
      expect(poolConfig!.connectionTimeoutMillis).toBe(3000);
      expect(poolConfig!.database).toBe('some_db_local');
    });

    test('local_suffix works without app_db_name', async () => {
      const localMockDBOSConfigYamlString = `
        name: some-app
        database:
          hostname: 'remote.com'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          local_suffix: true
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const poolConfig = dbosConfig.poolConfig;
      expect(poolConfig!.database).toBe('some_app_local');
    });

    test('local_suffix cannot be used with localhost', () => {
      const localMockDBOSConfigYamlString = `
        name: some-app
        database:
          hostname: 'localhost'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          local_suffix: true
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test('ssl defaults off for localhost', async () => {
      const localMockDBOSConfigYamlString = `
        database:
          hostname: 'localhost'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
          app_db_name: 'some_db'
        env:
          FOOFOO: barbar
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig!.ssl).toBe(false);
    });

    test('ssl defaults on for not-localhost', async () => {
      const localMockDBOSConfigYamlString = `
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
          app_db_name: 'some_db'
        env:
          FOOFOO: barbar
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig!.ssl).toEqual({ rejectUnauthorized: false });
    });

    test('getConfig throws when it finds a value of different type than the default', async () => {
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const dbosExec = new DBOSExecutor(dbosConfig);
      const ctx: WorkflowContextImpl = new WorkflowContextImpl(
        dbosExec,
        undefined,
        'testUUID',
        {},
        'testContext',
        true,
        undefined,
        undefined,
      );
      expect(() => ctx.getConfig<number>('payments_url', 1234)).toThrow(DBOSConfigKeyTypeError);
      // We didn't init, so do some manual cleanup only
      clearInterval(dbosExec.flushBufferID);
      await dbosExec.telemetryCollector.destroy();
    });

    test('parseConfigFile throws on an invalid config', async () => {
      const localMockDBOSConfigYamlString = `
      database:
        hosffftname: 'some host'
        porfft: 1234
        userffname: 'some user'
        passffword: \${PGPASSWORD}
        connfectionTimeoutMillis: 3000
        app_dfb_name: 'some_db'
    `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test('parseConfigFile disallows the user to be dbos', async () => {
      const localMockDBOSConfigYamlString = `
        database:
          hostname: 'some host'
          port: 1234
          username: 'dbos'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
          app_db_name: 'some_db'
        env:
          FOOFOO: barbar
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test('parseConfigFile throws on an invalid db name', async () => {
      const invalidNames = [
        'some_DB',
        '123db',
        'very_very_very_long_very_very_very_long_very_very__database_name',
        'largeDB',
        '',
      ];
      for (const dbName of invalidNames) {
        const localMockDBOSConfigYamlString = `
          database:
              hostname: 'some host'
              port: 1234
              username: 'some user'
              password: \${PGPASSWORD}
              app_db_name: '${dbName}'
        `;
        jest.restoreAllMocks();
        jest.spyOn(utils, 'readFileSync').mockReturnValue(localMockDBOSConfigYamlString);
        expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
      }
    });
  });

  describe('translatePublicDBOSconfig', () => {
    test('translate with full input', () => {
      const originalReadFileSync = fs.readFileSync;
      const certdata = 'abc';
      jest.spyOn(fs, 'readFileSync').mockImplementation((path, options) => {
        if (path === 'my_cert') {
          return certdata;
        }
        return originalReadFileSync(path, options);
      });

      const dbosConfig = {
        // Public fields
        name: 'dbostest',
        database_url: 'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
        userDbclient: UserDatabaseName.PRISMA,
        sysDbName: 'systemdbname',
        // app db pool size
        // sys db pool size
        logLevel: 'DEBUG',
        otlpTracesEndpoints: ['http://localhost:4317', 'unused.com'],
        adminPort: 666,
        runAdminServer: false,

        // Internal -- ignored -- fields
        poolConfig: {
          host: 'no',
          port: 777,
          user: 'no',
          password: 'no',
          database: 'no',
          connectionTimeoutMillis: 12345,
        },
        telemetry: {
          logs: {
            logLevel: 'WARN',
          },
          OTLPLogExporter: {
            logsEndPoint: 'youhou',
            tracesEndpoint: 'yadiyada',
          },
        },
        system_database: 'unused',
        env: {
          KEY: 'VALUE',
        },
        application: {
          counter: 3,
          shouldExist: 'exists',
        },
        http: {
          cors_middleware: true,
          credentials: false,
          allowed_origin: ['origin'],
        },
      };
      const [translatedDBOSConfig, translatedRuntimeConfig] = translatePublicDBOSconfig(dbosConfig);
      expect(translatedDBOSConfig).toEqual({
        name: 'dbostest',
        poolConfig: {
          host: 'mother',
          port: 2345,
          user: 'jon',
          password: 'doe',
          database: 'dbostest',
          connectionTimeoutMillis: 7000,
          ssl: { ca: [certdata], rejectUnauthorized: true },
        },
        userDbclient: UserDatabaseName.PRISMA,
        telemetry: {
          logs: {
            logLevel: 'DEBUG',
            forceConsole: false,
          },
          OTLPExporter: {
            tracesEndpoint: 'http://localhost:4317',
          },
        },
        system_database: 'systemdbname',
      });
      expect(translatedRuntimeConfig).toEqual({
        port: 3000,
        admin_port: 666,
        runAdminServer: false,
        entrypoints: [],
        start: [],
        setup: [],
      });
      jest.restoreAllMocks();
    });

    test('translate with no input', () => {
      const mockPackageJsoString = `{name: 'appname'}`;
      jest.spyOn(fs, 'readFileSync').mockReturnValue(mockPackageJsoString);
      const dbosConfig = {};
      const [translatedDBOSConfig, translatedRuntimeConfig] = translatePublicDBOSconfig(dbosConfig);
      expect(translatedDBOSConfig).toEqual({
        name: 'appname',
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: process.env.PGPASSWORD || 'dbos',
          database: 'appname',
          connectionTimeoutMillis: 3000,
          ssl: false,
        },
        userDbclient: UserDatabaseName.KNEX,
        telemetry: {
          logs: {
            logLevel: 'info',
            forceConsole: false,
          },
        },
        system_database: 'appname_dbos_sys',
      });
      expect(translatedRuntimeConfig).toEqual({
        port: 3000,
        admin_port: 3001,
        runAdminServer: true,
        entrypoints: [],
        start: [],
        setup: [],
      });
      jest.restoreAllMocks();
    });

    // name tests: no config file found, name in config & provided name differ, name is not provided but found in config, name is provided but no config file found
  });

  // TODO: I kind of want to pull out all the constructPoolConfig tests into their own suite, to rigourously test all paths
  //describe('constructPoolConfig', () => {
  //  test('no params', full default)
  // test mixed params
  // test env vars take precedence
  // });

  describe('parseDbString', () => {
    test('should correctly parse a full connection string with extra parameters', () => {
      // The parse function we use actually reads the certificate.
      jest.spyOn(fs, 'readFileSync').mockReturnValue('cert');
      const dbString =
        'postgres://user:password@localhost:5432/mydatabase?sslmode=require&sslrootcert=my_cert.pem&connect_timeout=5&extra_param=ignore_me';

      const result = parseDbString(dbString);

      expect(result).toEqual({
        hostname: 'localhost',
        port: 5432,
        username: 'user',
        password: 'password',
        app_db_name: 'mydatabase',
        ssl: true,
        ssl_ca: 'my_cert.pem',
        connectionTimeoutMillis: 5000,
      });
      jest.restoreAllMocks();
    });

    test('should parse a connection string with no parameters', () => {
      const dbString = 'postgres://user:password@localhost:5432/mydatabase';

      const result = parseDbString(dbString);

      expect(result).toEqual({
        hostname: 'localhost',
        port: 5432,
        username: 'user',
        password: 'password',
        app_db_name: 'mydatabase',
        ssl: false,
        ssl_ca: undefined,
        connectionTimeoutMillis: undefined,
      });
    });

    test('should parse a connection string with only some parameters', () => {
      const dbString = 'postgres://user@localhost:5432/mydatabase?sslmode=require';

      const result = parseDbString(dbString);

      expect(result).toEqual({
        hostname: 'localhost',
        port: 5432,
        username: 'user',
        password: undefined,
        app_db_name: 'mydatabase',
        ssl: true, // Since sslmode=require is present
        ssl_ca: undefined,
        connectionTimeoutMillis: undefined,
      });
    });

    test('should parse a connection string missing port', () => {
      const dbString = 'postgres://user:password@localhost/mydatabase';

      const result = parseDbString(dbString);

      expect(result).toEqual({
        hostname: 'localhost',
        port: undefined, // No port provided
        username: 'user',
        password: 'password',
        app_db_name: 'mydatabase',
        ssl: false,
        ssl_ca: undefined,
        connectionTimeoutMillis: undefined,
      });
    });

    test('should parse a connection string missing username and password', () => {
      const dbString = 'postgres://localhost:5432/mydatabase';

      const result = parseDbString(dbString);

      expect(result).toEqual({
        hostname: 'localhost',
        port: 5432,
        username: undefined,
        password: undefined,
        app_db_name: 'mydatabase',
        ssl: false,
        ssl_ca: undefined,
        connectionTimeoutMillis: undefined,
      });
    });

    test('should parse a connection string missing database name', () => {
      const dbString = 'postgres://user:password@localhost:5432';

      const result = parseDbString(dbString);

      expect(result).toEqual({
        hostname: 'localhost',
        port: 5432,
        username: 'user',
        password: 'password',
        app_db_name: undefined,
        ssl: false,
        ssl_ca: undefined,
        connectionTimeoutMillis: undefined,
      });
    });

    test('should handle an empty connection string (invalid case)', () => {
      expect(() => parseDbString('')).toThrow();
    });

    test('should handle an invalid connection string format', () => {
      expect(() => parseDbString('not-a-valid-db-string')).toThrow();
    });

    test('should handle a connection string missing hostname', () => {
      const dbString = 'postgres://user:password@:5432/mydatabase';
      expect(() => parseDbString(dbString)).toThrow();
    });
  });
});
