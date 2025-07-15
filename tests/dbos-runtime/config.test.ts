/* eslint-disable */

import fs from 'fs';
import * as utils from '../../src/utils';
import { UserDatabaseName } from '../../src/user_database';
import {
  ConfigFile,
  DBConfig,
  loadConfigFile,
  parseConfigFile,
  translatePublicDBOSconfig,
  overwrite_config,
  constructPoolConfig,
  dbosConfigFilePath,
  parseSSLConfig,
  processConfigFile,
  readConfigFile,
} from '../../src/dbos-runtime/config';
import { DBOSRuntimeConfig, defaultEntryPoint } from '../../src/dbos-runtime/runtime';
import { DBOSInitializationError } from '../../src/error';
import { DBOSConfig, DBOSConfigInternal } from '../../src/dbos-executor';
import { DBOSClient } from '../../src';
import { setUpDBOSTestDb } from '../helpers';

describe('dbos-config', () => {
  const mockCLIOptions = { port: NaN, loglevel: 'info' };
  const mockDBOSConfigYamlString = `
      name: 'some app'
      language: 'node'
      database:
        hostname: 'somehost'
        port: 1234
        username: 'someuser'
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
    test('loadConfigFile translates otlp endpoints from string to list', () => {
      const mockConfigFile = `
        database:
            hostname: \${DOESNOTEXISTS}
            port: \${DOESNOTEXISTS}
            username: \${DOESNOTEXISTS}
            password: \${NO}
            app_db_name: \${DOESNOTEXISTS}
            ssl: \${DOESNOTEXISTS}
            ssl_ca: \${DOESNOTEXISTS}
            connectionTimeoutMillis: \${DOESNOTEXISTS}
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

  describe('processConfigFile', () => {
    test('processConfigFile translates otlp endpoints from string to list', () => {
      const mockConfigFile = `
        name: 'test-app'
        telemetry:
            OTLPExporter:
                tracesEndpoint: http://otel-collector:4317/from-file
                logsEndpoint: http://otel-collector:4317/logs
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const configFile = readConfigFile();
      const [config] = processConfigFile(configFile, {});
      expect(config.telemetry.OTLPExporter?.tracesEndpoint).toEqual(['http://otel-collector:4317/from-file']);
      expect(config.telemetry.OTLPExporter?.logsEndpoint).toEqual(['http://otel-collector:4317/logs']);
    });

    test('processConfigFile logLevel default', () => {
      const mockConfigFile = `
        name: 'test-app'
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const configFile = readConfigFile();
      const [config] = processConfigFile(configFile, {});
      expect(config.telemetry.logs?.logLevel).toEqual('info');
    });

    test('processConfigFile logLevel specified', () => {
      const mockConfigFile = `
        name: 'test-app'
        telemetry:
            logs:
                logLevel: debug
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const configFile = readConfigFile();
      const [config] = processConfigFile(configFile, {});
      expect(config.telemetry.logs?.logLevel).toEqual('debug');
    });

    test('processConfigFile logLevel override', () => {
      const mockConfigFile = `
        name: 'test-app'
        telemetry:
            logs:
                logLevel: debug
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const configFile = readConfigFile();
      const [config] = processConfigFile(configFile, { loglevel: 'error' });
      expect(config.telemetry.logs?.logLevel).toEqual('error');
    });

    test('processConfigFile forceConsole override', () => {
      const mockConfigFile = `
        name: 'test-app'
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const configFile = readConfigFile();
      const [config] = processConfigFile(configFile, { forceConsole: true });
      expect(config.telemetry.logs?.forceConsole).toBeTruthy();
    });

    test("readConfigFile can't specify forceConsole", () => {
      const mockConfigFile = `
        name: 'test-app'
        telemetry:
            logs:
                forceOverride: true
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      expect(() => readConfigFile()).toThrow();
    });

    test('processConfigFile returns correct database url', () => {
      const mockConfigFile = `
        name: 'test-app'
        database_url: postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const configFile = readConfigFile();
      const [config] = processConfigFile(configFile, { forceConsole: true });
      expect(config.databaseUrl).toBe('postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable');
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
      expect(pool.connectionString).toBe(expected.connectionString);
      expect(pool.connectionTimeoutMillis).toBe(expected.connectionTimeoutMillis);
      expect(pool.ssl).toEqual(expected.ssl);
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
        connectionString: 'postgresql://postgres:dbos@localhost:5432/test_app?connect_timeout=10&sslmode=disable',
        connectionTimeoutMillis: 10000,
        ssl: false,
      });
    });

    test('gets db name from  pkg json name', () => {
      const mockPackageJsoString = `{"name": "appname"}`;
      jest.spyOn(fs, 'readFileSync').mockReturnValueOnce(mockPackageJsoString);
      const config = baseConfig();
      config.name = undefined;
      config.database ??= {};
      config.database.app_db_name = undefined;

      const pool = constructPoolConfig(config);

      assertPoolConfig(pool, {
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        password: 'dbos',
        database: 'appname',
        connectionTimeoutMillis: 10000,
        connectionString: 'postgresql://postgres:dbos@localhost:5432/appname?connect_timeout=10&sslmode=disable',
        ssl: false,
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
      config.database ??= {};
      config.database.app_db_name = 'appdb';
      config.database.ssl = false;
      config.database.hostname = 'something else';

      const pool = constructPoolConfig(config);

      assertPoolConfig(pool, {
        host: 'envhost',
        port: 7777,
        user: 'envuser',
        password: 'envpass',
        database: 'appdb',
        connectionString: 'postgresql://envuser:envpass@envhost:7777/appdb?connect_timeout=10&sslmode=disable',
        connectionTimeoutMillis: 10000,
        ssl: false,
      });
    });

    test('uses environment variable overrides with connection string input', () => {
      process.env.DBOS_DBHOST = 'envhost';
      process.env.DBOS_DBPORT = '7777';
      process.env.DBOS_DBUSER = 'envuser';
      process.env.DBOS_DBPASSWORD = 'envpass';
      process.env.DBOS_DEBUG_WORKFLOW_ID = 'debug-workflow-id';

      const config = baseConfig();
      config.database_url = 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable';

      const pool = constructPoolConfig(config);

      assertPoolConfig(pool, {
        host: 'envhost',
        port: 7777,
        user: 'envuser',
        password: 'envpass',
        database: 'appdb',
        connectionString: 'postgresql://envuser:envpass@envhost:7777/appdb?connect_timeout=22&sslmode=disable',
        connectionTimeoutMillis: 22000,
        ssl: false,
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
        max: 7, // from userDbPoolSize
        connectionTimeoutMillis: 10000, // default
        connectionString:
          'postgresql://configured_user:dbos@env-host.com:5432/configured_db?connect_timeout=10&sslmode=no-verify',
        ssl: { rejectUnauthorized: false },
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
        connectionTimeoutMillis: 10000,
        connectionString: 'postgresql://u:p@db:5432/test_app?connect_timeout=10&sslmode=verify-full&sslrootcert=ca.pem',
        ssl: { rejectUnauthorized: true, ca: ['CA_CERT'] },
      });
    });

    test('parses all connection parameters from database_url, backfill poolConfig and ignore provided configFile.database parameters', () => {
      const dbUrl = 'postgresql://url_user:url_pass@url_host:9999/url_db?sslmode=require&connect_timeout=15&extra=1';

      const config = baseConfig();
      config.database_url = dbUrl;
      config.database = {
        hostname: 'a',
        port: 1234,
        username: 'b',
      };
      const pool = constructPoolConfig(config);

      assertPoolConfig(pool, {
        host: 'url_host',
        port: 9999,
        user: 'url_user',
        password: 'url_pass',
        database: 'url_db',
        connectionString: dbUrl,
        connectionTimeoutMillis: 15000,
        ssl: { rejectUnauthorized: false },
      });
    });

    test('constructPoolConfig correctly handles app names with spaces', () => {
      const config = baseConfig();
      config.name = 'app name with spaces';
      config.database ??= {};
      config.database.ssl = true;
      const pool = constructPoolConfig(config);
      assertPoolConfig(pool, {
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        password: 'dbos',
        database: 'app_name_with_spaces',
        connectionTimeoutMillis: 10000,
        connectionString:
          'postgresql://postgres:dbos@localhost:5432/app_name_with_spaces?connect_timeout=10&sslmode=no-verify',
        ssl: { rejectUnauthorized: false },
      });
    });

    test('constructPoolConfig throws with invalid database_url format', () => {
      const config = baseConfig();
      config.database_url = 'not-a-valid-url';

      expect(() => constructPoolConfig(config)).toThrow();
    });

    test('constructPoolConfig throws when database_url is missing required fields', () => {
      // Test missing username and password
      const config2 = baseConfig();
      config2.database_url = 'postgres://host:5432/db';

      expect(() => constructPoolConfig(config2)).toThrow(/missing required field\(s\): username/);

      // Test missing hostname
      const config3 = baseConfig();
      config3.database_url = 'postgres://user:pass@:5432/db';

      expect(() => constructPoolConfig(config3)).toThrow(/Invalid URL/);

      // Test missing database name
      const config4 = baseConfig();
      config4.database_url = 'postgres://user:pass@host:5432/';

      expect(() => constructPoolConfig(config4)).toThrow(/missing required field\(s\): database name/);
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

      const [dbosConfig, runtimeConfig] = parseConfigFile(mockCLIOptions);

      // Test pool config options
      expect(dbosConfig.poolConfig).toBeDefined();

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
  });

  describe('context getConfig()', () => {
    beforeEach(() => {
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockDBOSConfigYamlString);
    });

    test('parseConfigFile throws on an invalid config', async () => {
      const localMockDBOSConfigYamlString = `
      database:
        hosffftname: 'some host'
        porfft: 1234
        userffname: 'some user'
        passffword: \${PGPASSWORD}
        connfectionTimeoutMillis: 10000
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
          connectionTimeoutMillis: 10000
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
          max: 20,
          connectionString:
            'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
          connectionTimeoutMillis: 7000,
          ssl: { rejectUnauthorized: false },
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
        sysDbPoolSize: 20,
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
          max: 20,
          connectionTimeoutMillis: 10000,
          connectionString: `postgresql://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/appname?connect_timeout=10&sslmode=disable`,
          ssl: false,
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
        sysDbPoolSize: 20,
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
      expect(resultDBOSConfig.poolConfig?.connectionString).toEqual(
        'postgresql://user-from-file:password-from-file@db-host-from-file:1234/db_from_file?connect_timeout=10&sslmode=no-verify',
      );

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

    test('should craft a verify-full address with an ssl_ca provided', () => {
      // Mock the config file content with database settings
      const mockDBOSConfigYamlString = `
            name: app-from-file
            database:
                hostname: db-host-from-file
                port: 1234
                username: user-from-file
                password: password-from-file
                app_db_name: db_from_file
                ssl_ca: my_cert
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
        telemetry: {},
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

      // Database settings should reflect what's in the file
      expect(resultDBOSConfig.poolConfig?.connectionString).toEqual(
        'postgresql://user-from-file:password-from-file@db-host-from-file:1234/db_from_file?connect_timeout=10&sslmode=verify-full&sslrootcert=my_cert',
      );
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

  describe('SSL Configuration Parsing', () => {
    const originalReadFileSync = fs.readFileSync;

    beforeEach(() => {
      jest.spyOn(utils, 'readFileSync').mockReturnValue('mock-certificate-data');
    });

    afterEach(() => {
      jest.restoreAllMocks();
    });

    test('parseSSLConfig returns false when SSL is explicitly disabled', () => {
      const dbConfig: DBConfig = {
        ssl: false,
      };
      expect(parseSSLConfig(dbConfig)).toBe(false);
    });

    test('parseSSLConfig returns certificate config when SSL CA is provided', () => {
      const dbConfig: DBConfig = {
        ssl: true,
        ssl_ca: '/path/to/ca.pem',
      };
      const result = parseSSLConfig(dbConfig);
      expect(result).toEqual({
        ca: ['mock-certificate-data'],
        rejectUnauthorized: true,
      });
      expect(utils.readFileSync).toHaveBeenCalledWith('/path/to/ca.pem');
    });

    test('parseSSLConfig disables SSL for localhost by default', () => {
      const dbConfig: DBConfig = {
        hostname: 'localhost',
      };
      expect(parseSSLConfig(dbConfig)).toBe(false);
    });

    test('parseSSLConfig disables SSL for 127.0.0.1 by default', () => {
      const dbConfig: DBConfig = {
        hostname: '127.0.0.1',
      };
      expect(parseSSLConfig(dbConfig)).toBe(false);
    });

    test('parseSSLConfig enables SSL with no verification for non-local hosts by default', () => {
      const dbConfig: DBConfig = {
        hostname: 'remote-db.example.com',
      };
      expect(parseSSLConfig(dbConfig)).toEqual({
        rejectUnauthorized: false,
      });
    });

    test('parseSSLConfig forces SSL for localhost when explicitly enabled', () => {
      const dbConfig: DBConfig = {
        hostname: 'localhost',
        ssl: true,
      };
      expect(parseSSLConfig(dbConfig)).toEqual({
        rejectUnauthorized: false,
      });
    });
  });

  describe('databaseUrl-no-password', () => {
    beforeAll(async () => {
      await setUpDBOSTestDb({});
    });

    test('No error when database_url is provided without password', async () => {
      const expected_url = 'postgresql://postgres@localhost:5432/dbostest?sslmode=disable';
      const config: DBOSConfig = {
        name: 'test-app',
        databaseUrl: expected_url,
      };
      const poolConfig = constructPoolConfig({
        database: {},
        database_url: expected_url,
        application: {},
        env: {},
      });
      expect(poolConfig.connectionString).toBe(expected_url);

      // Make sure we can use it to construct a client and connect to the database without the password.
      const client = await DBOSClient.create(expected_url);
      try {
        await expect(client.listQueuedWorkflows({})).resolves.toBeDefined();
      } finally {
        await client.destroy();
      }
    });
  });
});
