/* eslint-disable */

import fs from 'fs';
import * as utils from '../../src/utils';
import { UserDatabaseName } from '../../src/user_database';
import { PoolConfig } from 'pg';
import {
  ConfigFile,
  loadConfigFile,
  parseConfigFile,
  translatePublicDBOSconfig,
  overwrite_config,
  constructPoolConfig,
  dbosConfigFilePath,
} from '../../src/dbos-runtime/config';
import { DBOSRuntimeConfig, defaultEntryPoint } from '../../src/dbos-runtime/runtime';
import { DBOSConfigKeyTypeError, DBOSInitializationError } from '../../src/error';
import { DBOSExecutor, DBOSConfig, DBOSConfigInternal } from '../../src/dbos-executor';
import { WorkflowContextImpl } from '../../src/workflow';
import { get } from 'lodash';

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

  describe('Configuration loading', () => {
    test('translates otlp endpoints from string to list', () => {
      const mockConfigFile = `
        telemetry:
            OTLPExporter:
                tracesEndpoint: http://otel-collector:4317/from-file
                logsEndpoint: http://otel-collector:4317/logs
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const cfg: ConfigFile = loadConfigFile(dbosConfigFilePath);
      expect(cfg.telemetry?.OTLPExporter?.tracesEndpoint).toEqual(['http://otel-collector:4317/from-file']);
      expect(cfg.telemetry?.OTLPExporter?.logsEndpoint).toEqual(['http://otel-collector:4317/logs']);
    });
  });

  describe('constructPoolConfig', () => {
    beforeEach(() => {
      process.env = {};
    });

    const baseConfig = (): ConfigFile => ({
      name: 'Test App',
      application: {},
      env: {},
      database: {},
    });

    function assertPoolConfig(pool: any, expected: Partial<typeof pool>) {
      expect(pool.host).toBe(expected.host);
      expect(pool.port).toBe(expected.port);
      expect(pool.user).toBe(expected.user);
      expect(pool.password).toBe(expected.password);
      expect(pool.database).toBe(expected.database);
      expect(pool.connectionTimeoutMillis).toBe(expected.connectionTimeoutMillis);
      expect(pool.ssl).toEqual(expected.ssl);
      expect(pool.connectionString).toBe(expected.connectionString);
    }

    test('uses default values when config is empty', () => {
      const config = baseConfig();
      const pool = constructPoolConfig(config);

      assertPoolConfig(pool, {
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        password: 'dbos',
        database: 'test_app',
        connectionTimeoutMillis: 3000,
        connectionString:
          'postgresql://postgres:dbos@localhost:5432/test_app?connect_timeout=3&connection_limit=20&sslmode=disable',
      });
    });

    test('throws when app name and package.json are missing', () => {
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce('{}'); // load package.json

      const config = baseConfig();
      config.name = undefined;

      expect(() => constructPoolConfig(config)).toThrow(DBOSInitializationError);
    });

    test('uses environment variable overrides', () => {
      process.env.DBOS_DBHOST = 'envhost';
      process.env.DBOS_DBPORT = '7777';
      process.env.DBOS_DBUSER = 'envuser';
      process.env.DBOS_DBPASSWORD = 'envpass';

      const config = baseConfig();
      config.database.app_db_name = 'appdb';
      config.database.ssl = false;

      const pool = constructPoolConfig(config);

      assertPoolConfig(pool, {
        host: 'envhost',
        port: 7777,
        user: 'envuser',
        password: 'envpass',
        database: 'appdb',
        connectionTimeoutMillis: 3000,
        connectionString:
          'postgresql://envuser:envpass@envhost:7777/appdb?connect_timeout=3&connection_limit=20&sslmode=disable',
      });
    });

    test('uses mixed parameters from defaults, config, and environment', () => {
      process.env.DBOS_DBHOST = 'env-host.com';

      const config = baseConfig();
      config.database = {
        username: 'configured_user',
        app_db_name: 'configured_db',
      };

      const pool = constructPoolConfig(config, { userDbPoolSize: 7 });

      assertPoolConfig(pool, {
        host: 'env-host.com', // from DBOS_DBHOST
        port: 5432, // default
        user: 'configured_user', // from config.database
        password: 'dbos', // default
        database: 'configured_db', // from config.database
        connectionTimeoutMillis: 3000,
        max: 7, // from userDbPoolSize
        connectionString:
          'postgresql://configured_user:dbos@env-host.com:5432/configured_db?connect_timeout=3&connection_limit=7&sslmode=require',
      });
    });

    test('respects ssl_ca and builds verify-full connection string', () => {
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce('CA_CERT'); // readFileSync for CA

      const config = baseConfig();
      config.database = {
        hostname: 'db',
        port: 5432,
        username: 'u',
        password: 'p',
        ssl_ca: 'ca.pem',
      };

      const pool = constructPoolConfig(config);
      assertPoolConfig(pool, {
        host: 'db',
        port: 5432,
        user: 'u',
        password: 'p',
        database: 'test_app',
        connectionTimeoutMillis: 3000,
        connectionString:
          'postgresql://u:p@db:5432/test_app?connect_timeout=3&connection_limit=20&sslmode=verify-full&sslrootcert=ca.pem',
      });
    });

    // Note we still expect config.database to have been built off database_url.
    test('parses all connection parameters from database_url and ignores config.database', () => {
      const dbUrl = 'postgresql://url_user:url_pass@url_host:9999/url_db?sslmode=require&connect_timeout=15&extra=1';

      const config = baseConfig();
      config.database = {
        hostname: 'url_host',
        port: 9999,
        username: 'url_user',
        password: 'url_pass',
        app_db_name: 'url_db',
        connectionTimeoutMillis: 15000,
      };
      config.database_url = dbUrl;

      const pool = constructPoolConfig(config);

      assertPoolConfig(pool, {
        host: 'url_host',
        port: 9999,
        user: 'url_user',
        password: 'url_pass',
        database: 'url_db',
        connectionTimeoutMillis: 15000,
        connectionString: dbUrl,
      });
    });

    test('constructPoolConfig correctly handles app names with spaces', () => {
      const config = baseConfig();
      config.name = 'app name with spaces';
      config.database_url = 'postgresql://postgres:dbos@localhost:5432/';
      config.database.ssl = true;
      const pool = constructPoolConfig(config);
      assertPoolConfig(pool, {
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        password: 'dbos',
        database: 'app_name_with_spaces',
        connectionTimeoutMillis: 3000,
        connectionString: config.database_url,
      });
    });

    test('constructPoolConfig throws with invalid database_url format', () => {
      const config = baseConfig();
      config.database_url = 'not-a-valid-url';

      expect(() => constructPoolConfig(config)).toThrow();
    });

    test.only('constructPoolConfig throws when database_url is missing required fields', () => {
      // Test missing password
      const config1 = baseConfig();
      config1.database_url = 'postgres://user@host:5432/db';

      expect(() => constructPoolConfig(config1)).toThrow(/missing required field\(s\): password/);

      // Test missing username and password
      const config2 = baseConfig();
      config2.database_url = 'postgres://host:5432/db';

      expect(() => constructPoolConfig(config2)).toThrow(/missing required field\(s\): username, password/);

      // Test missing hostname
      const config3 = baseConfig();
      config3.database_url = 'postgres://user:pass@:5432/db';

      expect(() => constructPoolConfig(config3)).toThrow(/Invalid URL/);
    });
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
      expect(dbosConfig.poolConfig).toBeDefined();

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

    test('parseConfigFile prioritizes database_url over database field', () => {
      const localMockDBOSConfigYamlString = `
            name: some-app
            database_url: 'postgres://some_user:some_password@some_host:1234/some_db'
            database:
                hostname: 'localhost'
                port: 5432
                username: 'postgres'
                password: \${PGPASSWORD}
                app_db_name: 'some_db'
            `;
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(localMockDBOSConfigYamlString);
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce('SQL STATEMENTS');
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig!.host).toEqual('some_host');
      expect(dbosConfig.poolConfig!.port).toEqual(1234);
      expect(dbosConfig.poolConfig!.user).toEqual('some_user');
      expect(dbosConfig.poolConfig!.password).toEqual('some_password');
      expect(dbosConfig.poolConfig!.database).toEqual('some_db');
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
        ssl: false`;

      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);

      process.env.DBOS_DBHOST = 'DBHOST_OVERRIDE';
      process.env.DBOS_DBPORT = '99999';
      process.env.DBOS_DBUSER = 'DBUSER_OVERRIDE';
      process.env.DBOS_DBPASSWORD = 'DBPASSWORD_OVERRIDE';

      const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);

      const poolConfig: PoolConfig = dbosConfig.poolConfig!;
      expect(poolConfig.host).toBe('DBHOST_OVERRIDE');
      expect(poolConfig.port).toBe(99999);
      expect(poolConfig.user).toBe('DBUSER_OVERRIDE');
      expect(poolConfig.password).toBe('DBPASSWORD_OVERRIDE');
      expect(poolConfig.database).toBe('some_db');
    });

    test('constructPoolConfig correctly handles app names with spaces', () => {
      const mockDBOSConfigYamlString = `
        name: 'some app with spaces'
        `;

      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockDBOSConfigYamlString);

      const [dbosConfig, _]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const poolConfig: PoolConfig = dbosConfig.poolConfig!;
      expect(poolConfig.database).toBe('some_app_with_spaces');
    });
  });

  describe('context getConfig()', () => {
    beforeEach(() => {
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockDBOSConfigYamlString);
    });

    test('getConfig returns the expected values', async () => {
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfigInternal, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
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
      await dbosExec.telemetryCollector.destroy();
    });

    test('getConfig returns the default value when no application config is provided', async () => {
      const localMockDBOSConfigYamlString = `
        name: some-app
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
          app_db_name: 'some_db'
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfigInternal, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
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
      await dbosExec.telemetryCollector.destroy();
    });

    test('environment variables are set correctly', async () => {
      const localMockDBOSConfigYamlString = `
        name: some-app
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
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfigInternal, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const dbosExec = new DBOSExecutor(dbosConfig);
      expect(process.env.FOOFOO).toBe('barbar');
      expect(process.env.RANDENV).toBe(''); // Empty string
      await dbosExec.telemetryCollector.destroy();
    });

    test('getConfig throws when it finds a value of different type than the default', async () => {
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfigInternal, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
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
          name: 'some-app'
          database:
              hostname: 'some host'
              port: 1234
              username: 'some user'
              password: \${PGPASSWORD}
              app_db_name: '${dbName}'
        `;
        jest.restoreAllMocks();
        jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(localMockDBOSConfigYamlString);
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
        databaseUrl: 'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
        userDbclient: UserDatabaseName.PRISMA,
        sysDbName: 'systemdbname',
        logLevel: 'DEBUG',
        otlpTracesEndpoints: ['http://localhost:4317', 'unused.com'],
        otlpLogsEndpoints: ['http://localhost:4317', 'unused.com'],
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
      const [translatedDBOSConfig, translatedRuntimeConfig] = translatePublicDBOSconfig(dbosConfig, true);
      expect(translatedDBOSConfig).toEqual({
        name: dbosConfig.name, // provided name -- no config file was found
        poolConfig: {
          host: 'mother',
          port: 2345,
          user: 'jon',
          password: 'doe',
          database: 'dbostest',
          connectionTimeoutMillis: 7000,
          max: 20,
          connectionString:
            'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
        },
        userDbclient: UserDatabaseName.PRISMA,
        telemetry: {
          logs: {
            logLevel: dbosConfig.logLevel,
            forceConsole: true,
          },
          OTLPExporter: {
            tracesEndpoint: dbosConfig.otlpTracesEndpoints,
            logsEndpoint: dbosConfig.otlpLogsEndpoints,
          },
        },
        system_database: dbosConfig.sysDbName,
        sysDbPoolSize: 2,
      });
      expect(translatedRuntimeConfig).toEqual({
        port: 3000,
        admin_port: dbosConfig.adminPort,
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
      const dbosConfig = {
        databaseUrl: process.env.UNDEF,
      };
      const [translatedDBOSConfig, translatedRuntimeConfig] = translatePublicDBOSconfig(dbosConfig);
      expect(translatedDBOSConfig).toEqual({
        name: 'appname', // Found from config file
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: process.env.PGPASSWORD || 'dbos',
          database: 'appname',
          connectionTimeoutMillis: 3000,
          max: 20,
          connectionString:
            'postgresql://postgres:dbos@localhost:5432/appname?connect_timeout=3&connection_limit=20&sslmode=disable',
        },
        userDbclient: UserDatabaseName.KNEX,
        telemetry: {
          logs: {
            logLevel: 'info',
            forceConsole: false,
          },
          OTLPExporter: {
            tracesEndpoint: [],
            logsEndpoint: [],
          },
        },
        system_database: 'appname_dbos_sys',
        sysDbPoolSize: 2,
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

    test('translate with only deprecated, internal fields', () => {
      const mockPackageJsoString = `{name: 'appname'}`;
      jest.spyOn(fs, 'readFileSync').mockReturnValue(mockPackageJsoString);
      const dbosConfig = {
        poolConfig: {
          host: 'h',
          port: 123,
          user: 'u',
          password: 'p',
          database: 'd',
          connectionTimeoutMillis: 456,
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
        name: 'appname',
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: process.env.PGPASSWORD || 'dbos',
          database: 'appname',
          connectionTimeoutMillis: 3000,
          max: 20,
          connectionString:
            'postgresql://postgres:dbos@localhost:5432/appname?connect_timeout=3&connection_limit=20&sslmode=disable',
        },
        userDbclient: UserDatabaseName.KNEX,
        telemetry: {
          logs: {
            logLevel: 'info',
            forceConsole: false,
          },
          OTLPExporter: {
            tracesEndpoint: [],
            logsEndpoint: [],
          },
        },
        system_database: 'appname_dbos_sys',
        sysDbPoolSize: 2,
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

    test('fails when provided name conflicts with config file', () => {
      const mockPackageJsoString = `{name: 'appname'}`;
      jest.spyOn(fs, 'readFileSync').mockReturnValue(mockPackageJsoString);
      const dbosConfig = { name: 'differentappname' };
      expect(() => translatePublicDBOSconfig(dbosConfig)).toThrow(
        new DBOSInitializationError(
          "Provided app name 'differentappname' does not match the app name 'appname' in dbos-config.yaml",
        ),
      );
      jest.restoreAllMocks();
    });

    test('fails when config file is missing name field', () => {
      const mockPackageJsoString = `{}`;
      jest.spyOn(fs, 'readFileSync').mockReturnValue(mockPackageJsoString);
      const dbosConfig = {};
      expect(() => translatePublicDBOSconfig(dbosConfig)).toThrow(
        new DBOSInitializationError('Failed to load config from dbos-config.yaml: missing name field'),
      );
      jest.restoreAllMocks();
    });
  });

  describe('overwrite_config', () => {
    test('should return the original configs when config file is not found', () => {
      const providedDBOSConfig: DBOSConfigInternal = {
        name: 'test-app',
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: 'password',
          database: 'test_db',
        },
        telemetry: {},
        system_database: 'abc',
        userDbclient: UserDatabaseName.KNEX,
      };

      const providedRuntimeConfig: DBOSRuntimeConfig = {
        port: 3000,
        admin_port: 400,
        runAdminServer: false,
        entrypoints: ['app.js'],
        start: [],
        setup: [],
      };

      const [resultDBOSConfig, resultRuntimeConfig] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);

      // Should return the original configs unchanged
      expect(resultDBOSConfig).toEqual(providedDBOSConfig);
      expect(resultRuntimeConfig).toEqual(providedRuntimeConfig);
    });

    test('should throw when config file is empty', () => {
      jest.spyOn(utils, 'readFileSync').mockReturnValue('');

      const providedDBOSConfig: DBOSConfigInternal = {
        name: 'test-app',
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: 'password',
          database: 'test_db',
        },
        telemetry: {},
        system_database: 'abc',
        userDbclient: UserDatabaseName.KNEX,
      };

      const providedRuntimeConfig: DBOSRuntimeConfig = {
        port: 3000,
        admin_port: 400,
        runAdminServer: false,
        entrypoints: ['app.js'],
        start: [],
        setup: [],
      };

      expect(() => overwrite_config(providedDBOSConfig, providedRuntimeConfig)).toThrow(
        expect.objectContaining({
          message: expect.stringContaining('dbos-config.yaml is empty'),
        }),
      );
    });

    test('should throw when config file is invalid', () => {
      jest.spyOn(utils, 'readFileSync').mockReturnValue('{');

      const providedDBOSConfig: DBOSConfigInternal = {
        name: 'test-app',
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: 'password',
          database: 'test_db',
        },
        telemetry: {},
        system_database: 'abc',
        userDbclient: UserDatabaseName.KNEX,
      };

      const providedRuntimeConfig: DBOSRuntimeConfig = {
        port: 3000,
        admin_port: 400,
        runAdminServer: false,
        entrypoints: ['app.js'],
        start: [],
        setup: [],
      };

      expect(() => overwrite_config(providedDBOSConfig, providedRuntimeConfig)).toThrow();
    });

    test('should overwrite/merge parameters with config file content', () => {
      // Mock the config file content with database settings
      const mockDBOSConfigYamlString = `
          name: app-from-file
          database:
            hostname: db-host-from-file
            port: 1234
            username: user-from-file
            password: password-from-file
            app_db_name: db_from_file
            sys_db_name: sys_db_from_file
          telemetry:
            OTLPExporter:
                tracesEndpoint: http://otel-collector:4317/from-file
                logsEndpoint: http://otel-collector:4317/logs
            logs:
                logLevel: warning
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);

      const providedDBOSConfig: DBOSConfigInternal = {
        name: 'test-app',
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: 'password',
          database: 'test_db',
        },
        userDbclient: UserDatabaseName.KNEX,
        system_database: 'abc',
        telemetry: {
          logs: {
            logLevel: 'debug',
            forceConsole: false,
          },
          OTLPExporter: {
            tracesEndpoint: ['http://otel-collector:4317/original'],
            logsEndpoint: ['http://otel-collector:4317/yadiyada'],
          },
        },
      };

      const providedRuntimeConfig: DBOSRuntimeConfig = {
        port: 3000,
        admin_port: 4000,
        runAdminServer: false,
        entrypoints: ['app.js'],
        start: ['start.js'],
        setup: ['setup.js'],
      };

      const [resultDBOSConfig, resultRuntimeConfig] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);

      // App name should be from file
      expect(resultDBOSConfig.name).toEqual('app-from-file');

      // Database settings should reflect what's in the file
      expect(resultDBOSConfig.poolConfig?.host).toEqual('db-host-from-file');
      expect(resultDBOSConfig.poolConfig?.port).toEqual(1234);
      expect(resultDBOSConfig.poolConfig?.user).toEqual('user-from-file');
      expect(resultDBOSConfig.poolConfig?.password).toEqual('password-from-file');
      expect(resultDBOSConfig.poolConfig?.database).toEqual('db_from_file');

      // System database name should be from file
      expect(resultDBOSConfig.system_database).toEqual('sys_db_from_file');

      // Base telemetry config should be preserved from provided config
      expect(resultDBOSConfig.telemetry?.logs).toEqual(providedDBOSConfig.telemetry?.logs);
      // OTLP endpoints should be overwritten from the file
      expect(resultDBOSConfig.telemetry?.OTLPExporter?.tracesEndpoint).toEqual([
        'http://otel-collector:4317/original',
        'http://otel-collector:4317/from-file',
      ]);
      expect(resultDBOSConfig.telemetry?.OTLPExporter?.logsEndpoint).toEqual([
        'http://otel-collector:4317/yadiyada',
        'http://otel-collector:4317/logs',
      ]);

      // Other provided fields should be preserved
      expect(resultDBOSConfig.userDbclient).toEqual(providedDBOSConfig.userDbclient);

      // Runtime admin_port and runAdminServer should be overwritten
      expect(resultRuntimeConfig.admin_port).toEqual(3001);
      expect(resultRuntimeConfig.runAdminServer).toBe(true);

      // Other runtime settings should be preserved
      expect(resultRuntimeConfig.port).toEqual(providedRuntimeConfig.port);
      expect(resultRuntimeConfig.entrypoints).toEqual(providedRuntimeConfig.entrypoints);
      expect(resultRuntimeConfig.start).toEqual(providedRuntimeConfig.start);
      expect(resultRuntimeConfig.setup).toEqual(providedRuntimeConfig.setup);
    });

    test('should use config file content when parameters are missing from provided config', () => {
      // Mock the config file content with database settings
      const mockDBOSConfigYamlString = `
            name: app-from-file
            database:
                hostname: db-host-from-file
                port: 1234
                username: user-from-file
                password: password-from-file
                app_db_name: db_from_file
                sys_db_name: sys_db_from_file
            telemetry:
                OTLPExporter:
                    tracesEndpoint: http://otel-collector:4317/from-file
                logs:
                    logLevel: warning
            `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);

      const providedDBOSConfig: DBOSConfigInternal = {
        name: 'test-app',
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: 'password',
          database: 'test_db',
        },
        userDbclient: UserDatabaseName.KNEX,
        system_database: 'abc',
        telemetry: {
          logs: {
            logLevel: 'debug',
            forceConsole: false,
          },
        },
      };
      const providedRuntimeConfig: DBOSRuntimeConfig = {
        port: 3000,
        admin_port: 4000,
        runAdminServer: false,
        entrypoints: ['app.js'],
        start: ['start.js'],
        setup: ['setup.js'],
      };

      const [resultDBOSConfig] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);

      // Base telemetry config should be preserved from provided config
      expect(resultDBOSConfig.telemetry?.logs).toEqual(providedDBOSConfig.telemetry?.logs);
      // OTLP traces endpoint are overwritten from the file
      expect(resultDBOSConfig.telemetry?.OTLPExporter?.tracesEndpoint).toEqual([
        'http://otel-collector:4317/from-file',
      ]);
    });

    test('should use provided config when parameters are missing from config file', () => {
      // Mock the config file without telemetry settings
      const mockDBOSConfigYamlString = `
          database:
            hostname: db-host-from-file
            port: 1234
            username: user-from-file
            password: password-from-file
            app_db_name: db_from_file
            sys_db_name: sys_db_from_file
        `;

      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockDBOSConfigYamlString);

      const providedDBOSConfig: DBOSConfigInternal = {
        name: 'test-app',
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: 'password',
          database: 'test_db',
        },
        userDbclient: UserDatabaseName.KNEX,
        system_database: 'abc',
        telemetry: {
          logs: {
            logLevel: 'info',
            forceConsole: false,
          },
          OTLPExporter: {
            tracesEndpoint: ['http://otel-collector:4317/original'],
            logsEndpoint: ['http://otel-collector:4317/yadiyada'],
          },
        },
      };

      const providedRuntimeConfig: DBOSRuntimeConfig = {
        port: 3000,
        admin_port: 4000,
        runAdminServer: false,
        entrypoints: ['app.js'],
        start: [],
        setup: [],
      };

      const [resultDBOSConfig] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);
      expect(resultDBOSConfig.telemetry).toEqual(providedDBOSConfig.telemetry);
      expect(resultDBOSConfig.name).toEqual('test-app');
      expect(resultDBOSConfig.telemetry?.OTLPExporter).toEqual(providedDBOSConfig.telemetry?.OTLPExporter);
    });

    test('use constructPoolConfig default sys db name when missing from config file', () => {
      // Mock config file without sys_db_name
      const mockDBOSConfigYamlString = `
      name: app-from-file
      database:
        hostname: db-host-from-file
        port: 1234
        username: user-from-file
        password: password-from-file
        app_db_name: db_from_file
    `;

      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockDBOSConfigYamlString);

      const providedDBOSConfig: DBOSConfigInternal = {
        name: 'test-app',
        poolConfig: {
          host: 'localhost',
          port: 5432,
          user: 'postgres',
          password: 'password',
          database: 'test_db',
        },
        telemetry: {},
        system_database: 'original_sys_db',
      };

      const providedRuntimeConfig: DBOSRuntimeConfig = {
        port: 3000,
        admin_port: 3001,
        runAdminServer: true,
        entrypoints: [],
        start: [],
        setup: [],
      };

      const [resultDBOSConfig, _] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);
      expect(resultDBOSConfig.system_database).toEqual(`${resultDBOSConfig.poolConfig?.database}_dbos_sys`);
    });
  });
});
