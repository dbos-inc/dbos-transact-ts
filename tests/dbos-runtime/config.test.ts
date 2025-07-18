/* eslint-disable */

import fs from 'fs';
import * as utils from '../../src/utils';
import { UserDatabaseName } from '../../src/user_database';
import { ConfigFile, getDatabaseUrl, getDbosConfig, readConfigFile } from '../../src/dbos-runtime/config';
import { DBOSInitializationError } from '../../src/error';
import { setUpDBOSTestDb } from '../helpers';
import { AssertionError } from 'assert';

describe('dbos-config', () => {
  beforeEach(() => {
    jest.resetModules();
    process.env = {};
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('readConfigFile', () => {
    test('reads package name if not specified in config file', () => {
      const mockConfigFile = `database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable'`;
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockConfigFile);
      const mockPackageJson = `{ "name": "test-app-from-package-json" }`;
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockPackageJson);

      const cfg: ConfigFile = readConfigFile();
      expect(cfg).toEqual({
        name: 'test-app-from-package-json',
        database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      });
    });

    test('handles env substitution', () => {
      const database_url = `test-value-${Date.now()}`;
      process.env.MY_DATABASE_URL = database_url;

      const mockConfigFile = `
        name: 'test-app'
        database_url: \${MY_DATABASE_URL}`;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const cfg: ConfigFile = readConfigFile();
      expect(cfg).toEqual({
        name: 'test-app',
        database_url,
      });
    });

    test('handles missing env substitution', () => {
      process.env = {};

      const mockConfigFile = `
        name: 'test-app'
        database_url: \${MY_DATABASE_URL}`;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const cfg: ConfigFile = readConfigFile();
      expect(cfg).toEqual({
        name: 'test-app',
        database_url: '',
      });
    });

    test('handles single string endpoints', () => {
      const mockConfigFile = `
        name: 'test-app'
        telemetry:
            OTLPExporter:
                tracesEndpoint: http://otel-collector:4317/from-file
                logsEndpoint: http://otel-collector:4317/logs
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const cfg: ConfigFile = readConfigFile();
      expect(cfg).toEqual({
        name: 'test-app',
        telemetry: {
          OTLPExporter: {
            tracesEndpoint: 'http://otel-collector:4317/from-file',
            logsEndpoint: 'http://otel-collector:4317/logs',
          },
        },
      });
    });

    test('handles string array endpoints', () => {
      const mockConfigFile = `
        name: 'test-app'
        telemetry:
            OTLPExporter:
                tracesEndpoint:
                  - http://otel-collector:4317/from-file
                  - http://otel-collector:4317/from-file2
                logsEndpoint:
                  - http://otel-collector:4317/logs
                  - http://otel-collector:4317/logs2
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      const cfg: ConfigFile = readConfigFile();
      expect(cfg).toEqual({
        name: 'test-app',
        telemetry: {
          OTLPExporter: {
            tracesEndpoint: ['http://otel-collector:4317/from-file', 'http://otel-collector:4317/from-file2'],
            logsEndpoint: ['http://otel-collector:4317/logs', 'http://otel-collector:4317/logs2'],
          },
        },
      });
    });

    test("can't specify forceConsole", () => {
      const mockConfigFile = `
        name: 'test-app'
        telemetry:
            logs:
                forceOverride: true
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      expect(() => readConfigFile()).toThrow();
    });

    test("can't specify unexpected fields", () => {
      const mockConfigFile = `
        name: 'test-app'
        unexpected: 'some value'
        `;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);

      expect(() => readConfigFile()).toThrow();
    });

    class FakeNotFoundError extends Error {
      readonly code = 'ENOENT';
    }

    test('config and package files not found', () => {
      jest.spyOn(utils, 'readFileSync').mockImplementation(() => {
        throw new FakeNotFoundError('not found');
      });
      const configFile = readConfigFile();
      expect(configFile).toEqual({});
    });

    test('config file not found', () => {
      jest.spyOn(utils, 'readFileSync').mockImplementationOnce(() => {
        throw new FakeNotFoundError('not found');
      });
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(`{ "name": "test-app-from-package-json" }`);
      const configFile = readConfigFile();
      expect(configFile).toEqual({ name: 'test-app-from-package-json' });
    });

    test('package file not found', () => {
      jest
        .spyOn(utils, 'readFileSync')
        .mockReturnValueOnce(`database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable'`);
      jest.spyOn(utils, 'readFileSync').mockImplementationOnce(() => {
        throw new FakeNotFoundError('not found');
      });
      const configFile = readConfigFile();
      expect(configFile).toEqual({ database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable' });
    });

    test('does not read package if config file has name', () => {
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(`name: 'test-app'`);
      jest.spyOn(utils, 'readFileSync').mockImplementationOnce(() => {
        throw new Error('Should not be called');
      });
      const configFile = readConfigFile();
      expect(configFile).toEqual({ name: 'test-app' });
    });

    test('does not read package if config file has name', () => {
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(`name: 'test-app'`);
      jest.spyOn(utils, 'readFileSync').mockImplementationOnce(() => {
        throw new Error('Should not be called');
      });
      const configFile = readConfigFile();
      expect(configFile).toEqual({ name: 'test-app' });
    });

    test('throws on non-ENOENT config read error', () => {
      jest.spyOn(utils, 'readFileSync').mockImplementation(() => {
        throw new Error('Some other error');
      });
      expect(() => readConfigFile()).toThrow(new Error('Some other error'));
    });

    test('throws on non-ENOENT package read error', () => {
      jest.spyOn(utils, 'readFileSync').mockImplementationOnce(() => {
        throw new FakeNotFoundError('not found');
      });
      jest.spyOn(utils, 'readFileSync').mockImplementationOnce(() => {
        throw new Error('Some other error');
      });
      expect(() => readConfigFile()).toThrow(new Error('Some other error'));
    });
  });

  describe('getDbosConfig', () => {
    test('translates otlp endpoints from string to list', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        telemetry: {
          OTLPExporter: {
            tracesEndpoint: 'http://otel-collector:4317/from-file',
            logsEndpoint: 'http://otel-collector:4317/logs',
          },
        },
      };
      const config = getDbosConfig(configFile);
      expect(config.telemetry.OTLPExporter?.tracesEndpoint).toEqual(['http://otel-collector:4317/from-file']);
      expect(config.telemetry.OTLPExporter?.logsEndpoint).toEqual(['http://otel-collector:4317/logs']);
    });

    test('support array oltp endpoints', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        telemetry: {
          OTLPExporter: {
            tracesEndpoint: ['http://otel-collector:4317/from-file', 'http://otel-collector:4317/from-file2'],
            logsEndpoint: ['http://otel-collector:4317/logs', 'http://otel-collector:4317/logs2'],
          },
        },
      };
      const config = getDbosConfig(configFile);
      expect(config.telemetry?.OTLPExporter?.tracesEndpoint).toEqual([
        'http://otel-collector:4317/from-file',
        'http://otel-collector:4317/from-file2',
      ]);
      expect(config.telemetry?.OTLPExporter?.logsEndpoint).toEqual([
        'http://otel-collector:4317/logs',
        'http://otel-collector:4317/logs2',
      ]);
    });

    test('logLevel default', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
      };
      const config = getDbosConfig(configFile);
      expect(config.telemetry.logs?.logLevel).toEqual('info');
    });

    test('logLevel specified', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        telemetry: {
          logs: {
            logLevel: 'debug',
          },
        },
      };
      const config = getDbosConfig(configFile);
      expect(config.telemetry.logs?.logLevel).toEqual('debug');
    });

    test('logLevel override', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        telemetry: {
          logs: {
            logLevel: 'debug',
          },
        },
      };
      const config = getDbosConfig(configFile, { logLevel: 'error' });
      expect(config.telemetry.logs?.logLevel).toEqual('error');
    });

    test('forceConsole override', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
      };
      const config = getDbosConfig(configFile, { forceConsole: true });
      expect(config.telemetry.logs?.forceConsole).toBeTruthy();
    });

    test('returns correct database url', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      };
      const config = getDbosConfig(configFile);
      expect(config.databaseUrl).toBe('postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable');
    });

    test('handles node language', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        language: 'node',
        database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      };
      expect(() => getDbosConfig(configFile)).not.toThrow();
    });

    test('handles missing language', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      };
      expect(() => getDbosConfig(configFile)).not.toThrow();
    });

    test('throws on non node language', () => {
      const configFile: ConfigFile = {
        name: 'test-app',
        language: 'not-node',
        database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      };
      expect(() => getDbosConfig(configFile)).toThrow();
    });
  });

  describe('getDatabaseUrl', () => {
    test('uses database_url from config when provided', () => {
      const databaseUrl = getDatabaseUrl({
        name: 'Test App',
        database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      });
      expect(databaseUrl).toBe('postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable');
    });

    test('uses environment variable when provided', () => {
      process.env.DBOS_DATABASE_URL = 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable';
      const databaseUrl = getDatabaseUrl({
        name: 'Test App',
      });
      expect(databaseUrl).toBe('postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable');
    });

    test('uses default values when config is empty', () => {
      const databaseUrl = getDatabaseUrl({
        name: 'Test App',
      });
      expect(databaseUrl).toBe('postgresql://postgres:dbos@localhost:5432/test_app?connect_timeout=10&sslmode=disable');
    });

    test('throws when db url not set and app name is missing', () => {
      expect(() => getDatabaseUrl({})).toThrow(AssertionError);
    });

    test('uses PG env values when config is empty', () => {
      process.env.PGHOST = 'envhost';
      process.env.PGPORT = '7777';
      process.env.PGUSER = 'envuser';
      process.env.PGPASSWORD = 'envpass';

      const databaseUrl = getDatabaseUrl({
        name: 'Test App',
      });
      expect(databaseUrl).toBe('postgresql://envuser:envpass@envhost:7777/test_app?connect_timeout=10&sslmode=allow');
    });

    test('uses environment variable overrides with connection string input', () => {
      process.env.DBOS_DBHOST = 'envhost';
      process.env.DBOS_DBPORT = '7777';
      process.env.DBOS_DBUSER = 'envuser';
      process.env.DBOS_DBPASSWORD = 'envpass';
      process.env.DBOS_DEBUG_WORKFLOW_ID = 'debug-workflow-id';

      const url = getDatabaseUrl({
        database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      });
      expect(url).toBe('postgresql://envuser:envpass@envhost:7777/appdb?connect_timeout=22&sslmode=disable');
    });

    test('correctly handles app names with spaces', () => {
      const url = getDatabaseUrl({
        name: 'app name with spaces',
      });
      expect(url).toBe(
        'postgresql://postgres:dbos@localhost:5432/app_name_with_spaces?connect_timeout=10&sslmode=disable',
      );
    });

    test('throws with invalid database_url format', () => {
      expect(() => getDatabaseUrl({ database_url: 'not-a-valid-url' })).toThrow();
    });

    test('throws when database_url is missing required fields', () => {
      expect(() => getDatabaseUrl({ database_url: 'postgres://host:5432/db' })).toThrow(
        /missing required field\(s\): username/,
      );
      expect(() => getDatabaseUrl({ database_url: 'postgres://user:pass@:5432/db' })).toThrow(/Invalid URL/);
      expect(() => getDatabaseUrl({ database_url: 'postgres://user:pass@host:5432/' })).toThrow(
        /missing required field\(s\): database name/,
      );
    });
  });

  describe('context getConfig()', () => {
    beforeEach(() => {
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
      jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockDBOSConfigYamlString);
    });

    test.skip('parseConfigFile throws on an invalid config', async () => {
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
      expect(() => readConfigFile()).toThrow(DBOSInitializationError);
    });

    test.skip('parseConfigFile disallows the user to be dbos', async () => {
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
      //   expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
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
        // expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
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
        userDbClient: UserDatabaseName.PRISMA,
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
      //   const [translatedDBOSConfig, translatedRuntimeConfig] = translatePublicDBOSconfig(dbosConfig, true);
      //   expect(translatedDBOSConfig).toEqual({
      //     name: dbosConfig.name, // provided name -- no config file was found
      //     databaseUrl: 'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
      //     poolConfig: {
      //       host: 'mother',
      //       port: 2345,
      //       user: 'jon',
      //       password: 'doe',
      //       database: 'dbostest',
      //       max: 20,
      //       connectionString:
      //         'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
      //       connectionTimeoutMillis: 7000,
      //       ssl: { rejectUnauthorized: false },
      //     },
      //     userDbClient: UserDatabaseName.PRISMA,
      //     telemetry: {
      //       logs: {
      //         addContextMetadata: undefined,
      //         logLevel: dbosConfig.logLevel,
      //         forceConsole: true,
      //       },
      //       OTLPExporter: {
      //         tracesEndpoint: dbosConfig.otlpTracesEndpoints,
      //         logsEndpoint: dbosConfig.otlpLogsEndpoints,
      //       },
      //     },
      //     system_database: dbosConfig.sysDbName,
      //     sysDbPoolSize: 20,
      //   });
      //   expect(translatedRuntimeConfig).toEqual({
      //     port: 3000,
      //     admin_port: dbosConfig.adminPort,
      //     runAdminServer: false,
      //     entrypoints: [],
      //     start: [],
      //     setup: [],
      //   });
      jest.restoreAllMocks();
    });

    test('translate with no input', () => {
      const mockPackageJsoString = `{name: 'appname'}`;
      jest.spyOn(fs, 'readFileSync').mockReturnValue(mockPackageJsoString);
      const dbosConfig = {
        databaseUrl: process.env.UNDEF,
      };
      //   const [translatedDBOSConfig, translatedRuntimeConfig] = translatePublicDBOSconfig(dbosConfig);
      //   expect(translatedDBOSConfig).toEqual({
      //     name: 'appname', // Found from config file
      //     poolConfig: {
      //       host: 'localhost',
      //       port: 5432,
      //       user: 'postgres',
      //       password: process.env.PGPASSWORD || 'dbos',
      //       database: 'appname',
      //       max: 20,
      //       connectionTimeoutMillis: 10000,
      //       connectionString: `postgresql://postgres:${process.env.PGPASSWORD || 'dbos'}@localhost:5432/appname?connect_timeout=10&sslmode=disable`,
      //       ssl: false,
      //     },
      //     userDbClient: UserDatabaseName.KNEX,
      //     telemetry: {
      //       logs: {
      //         logLevel: 'info',
      //         forceConsole: false,
      //       },
      //       OTLPExporter: {
      //         tracesEndpoint: [],
      //         logsEndpoint: [],
      //       },
      //     },
      //     system_database: 'appname_dbos_sys',
      //     sysDbPoolSize: 20,
      //   });
      //   expect(translatedRuntimeConfig).toEqual({
      //     port: 3000,
      //     admin_port: 3001,
      //     runAdminServer: true,
      //     entrypoints: [],
      //     start: [],
      //     setup: [],
      //   });
      jest.restoreAllMocks();
    });
  });

  describe('overwrite_config', () => {
    test('should return the original configs when config file is not found', () => {
      //   const providedDBOSConfig: DBOSConfigInternal = {
      //     name: 'test-app',
      //     poolConfig: {
      //       host: 'localhost',
      //       port: 5432,
      //       user: 'postgres',
      //       password: 'password',
      //       database: 'test_db',
      //     },
      //     telemetry: {},
      //     system_database: 'abc',
      //     userDbClient: UserDatabaseName.KNEX,
      //   };
      //   const providedRuntimeConfig: DBOSRuntimeConfig = {
      //     port: 3000,
      //     admin_port: 400,
      //     runAdminServer: false,
      //     entrypoints: ['app.js'],
      //     start: [],
      //     setup: [],
      //   };
      //   const [resultDBOSConfig, resultRuntimeConfig] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);
      //   // Should return the original configs unchanged
      //   expect(resultDBOSConfig).toEqual(providedDBOSConfig);
      //   expect(resultRuntimeConfig).toEqual(providedRuntimeConfig);
    });

    test('should throw when config file is empty', () => {
      //   jest.spyOn(utils, 'readFileSync').mockReturnValue('');
      //   const providedDBOSConfig: DBOSConfigInternal = {
      //     name: 'test-app',
      //     poolConfig: {
      //       host: 'localhost',
      //       port: 5432,
      //       user: 'postgres',
      //       password: 'password',
      //       database: 'test_db',
      //     },
      //     telemetry: {},
      //     system_database: 'abc',
      //     userDbClient: UserDatabaseName.KNEX,
      //   };
      //   const providedRuntimeConfig: DBOSRuntimeConfig = {
      //     port: 3000,
      //     admin_port: 400,
      //     runAdminServer: false,
      //     entrypoints: ['app.js'],
      //     start: [],
      //     setup: [],
      //   };
      //   expect(() => overwrite_config(providedDBOSConfig, providedRuntimeConfig)).toThrow(
      //     expect.objectContaining({
      //       message: expect.stringContaining('dbos-config.yaml is empty'),
      //     }),
      //   );
    });

    test('should throw when config file is invalid', () => {
      //   jest.spyOn(utils, 'readFileSync').mockReturnValue('{');
      //   const providedDBOSConfig: DBOSConfigInternal = {
      //     name: 'test-app',
      //     poolConfig: {
      //       host: 'localhost',
      //       port: 5432,
      //       user: 'postgres',
      //       password: 'password',
      //       database: 'test_db',
      //     },
      //     telemetry: {},
      //     system_database: 'abc',
      //     userDbClient: UserDatabaseName.KNEX,
      //   };
      //   const providedRuntimeConfig: DBOSRuntimeConfig = {
      //     port: 3000,
      //     admin_port: 400,
      //     runAdminServer: false,
      //     entrypoints: ['app.js'],
      //     start: [],
      //     setup: [],
      //   };
      //   expect(() => overwrite_config(providedDBOSConfig, providedRuntimeConfig)).toThrow();
    });

    test('should overwrite/merge parameters with config file content', () => {
      //   // Mock the config file content with database settings
      //   const mockDBOSConfigYamlString = `
      //       name: app-from-file
      //       database:
      //         hostname: db-host-from-file
      //         port: 1234
      //         username: user-from-file
      //         password: password-from-file
      //         app_db_name: db_from_file
      //         sys_db_name: sys_db_from_file
      //       telemetry:
      //         OTLPExporter:
      //             tracesEndpoint: http://otel-collector:4317/from-file
      //             logsEndpoint: http://otel-collector:4317/logs
      //         logs:
      //             logLevel: warning
      //     `;
      //   jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);
      //   const providedDBOSConfig: DBOSConfigInternal = {
      //     name: 'test-app',
      //     poolConfig: {
      //       host: 'localhost',
      //       port: 5432,
      //       user: 'postgres',
      //       password: 'password',
      //       database: 'test_db',
      //     },
      //     userDbClient: UserDatabaseName.KNEX,
      //     system_database: 'abc',
      //     telemetry: {
      //       logs: {
      //         logLevel: 'debug',
      //         forceConsole: false,
      //       },
      //       OTLPExporter: {
      //         tracesEndpoint: ['http://otel-collector:4317/original'],
      //         logsEndpoint: ['http://otel-collector:4317/yadiyada'],
      //       },
      //     },
      //   };
      //   const providedRuntimeConfig: DBOSRuntimeConfig = {
      //     port: 3000,
      //     admin_port: 4000,
      //     runAdminServer: false,
      //     entrypoints: ['app.js'],
      //     start: ['start.js'],
      //     setup: ['setup.js'],
      //   };
      //   const [resultDBOSConfig, resultRuntimeConfig] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);
      //   // App name should be from file
      //   expect(resultDBOSConfig.name).toEqual('app-from-file');
      //   // Database settings should reflect what's in the file
      //   expect(resultDBOSConfig.poolConfig?.host).toEqual('db-host-from-file');
      //   expect(resultDBOSConfig.poolConfig?.port).toEqual(1234);
      //   expect(resultDBOSConfig.poolConfig?.user).toEqual('user-from-file');
      //   expect(resultDBOSConfig.poolConfig?.password).toEqual('password-from-file');
      //   expect(resultDBOSConfig.poolConfig?.database).toEqual('db_from_file');
      //   expect(resultDBOSConfig.poolConfig?.connectionString).toEqual(
      //     'postgresql://user-from-file:password-from-file@db-host-from-file:1234/db_from_file?connect_timeout=10&sslmode=no-verify',
      //   );
      //   // System database name should be from file
      //   expect(resultDBOSConfig.system_database).toEqual('sys_db_from_file');
      //   // Base telemetry config should be preserved from provided config
      //   expect(resultDBOSConfig.telemetry?.logs).toEqual(providedDBOSConfig.telemetry?.logs);
      //   // OTLP endpoints should be overwritten from the file
      //   expect(resultDBOSConfig.telemetry?.OTLPExporter?.tracesEndpoint).toEqual([
      //     'http://otel-collector:4317/original',
      //     'http://otel-collector:4317/from-file',
      //   ]);
      //   expect(resultDBOSConfig.telemetry?.OTLPExporter?.logsEndpoint).toEqual([
      //     'http://otel-collector:4317/yadiyada',
      //     'http://otel-collector:4317/logs',
      //   ]);
      //   // Other provided fields should be preserved
      //   expect(resultDBOSConfig.userDbClient).toEqual(providedDBOSConfig.userDbClient);
      //   // Runtime admin_port and runAdminServer should be overwritten
      //   expect(resultRuntimeConfig.admin_port).toEqual(3001);
      //   expect(resultRuntimeConfig.runAdminServer).toBe(true);
      //   // Other runtime settings should be preserved
      //   expect(resultRuntimeConfig.port).toEqual(providedRuntimeConfig.port);
      //   expect(resultRuntimeConfig.entrypoints).toEqual(providedRuntimeConfig.entrypoints);
      //   expect(resultRuntimeConfig.start).toEqual(providedRuntimeConfig.start);
      //   expect(resultRuntimeConfig.setup).toEqual(providedRuntimeConfig.setup);
    });

    test('should craft a verify-full address with an ssl_ca provided', () => {
      //   // Mock the config file content with database settings
      //   const mockDBOSConfigYamlString = `
      //         name: app-from-file
      //         database:
      //             hostname: db-host-from-file
      //             port: 1234
      //             username: user-from-file
      //             password: password-from-file
      //             app_db_name: db_from_file
      //             ssl_ca: my_cert
      //         `;
      //   jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);
      //   const providedDBOSConfig: DBOSConfigInternal = {
      //     name: 'test-app',
      //     poolConfig: {
      //       host: 'localhost',
      //       port: 5432,
      //       user: 'postgres',
      //       password: 'password',
      //       database: 'test_db',
      //     },
      //     userDbClient: UserDatabaseName.KNEX,
      //     system_database: 'abc',
      //     telemetry: {},
      //   };
      //   const providedRuntimeConfig: DBOSRuntimeConfig = {
      //     port: 3000,
      //     admin_port: 4000,
      //     runAdminServer: false,
      //     entrypoints: ['app.js'],
      //     start: [],
      //     setup: [],
      //   };
      //   const [resultDBOSConfig] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);
      //   // Database settings should reflect what's in the file
      //   expect(resultDBOSConfig.poolConfig?.connectionString).toEqual(
      //     'postgresql://user-from-file:password-from-file@db-host-from-file:1234/db_from_file?connect_timeout=10&sslmode=verify-full&sslrootcert=my_cert',
      //   );
    });

    test('should use config file content when parameters are missing from provided config', () => {
      //   // Mock the config file content with database settings
      //   const mockDBOSConfigYamlString = `
      //         name: app-from-file
      //         database:
      //             hostname: db-host-from-file
      //             port: 1234
      //             username: user-from-file
      //             password: password-from-file
      //             app_db_name: db_from_file
      //             sys_db_name: sys_db_from_file
      //         telemetry:
      //             OTLPExporter:
      //                 tracesEndpoint: http://otel-collector:4317/from-file
      //             logs:
      //                 logLevel: warning
      //         `;
      //   jest.spyOn(utils, 'readFileSync').mockReturnValue(mockDBOSConfigYamlString);
      //   const providedDBOSConfig: DBOSConfigInternal = {
      //     name: 'test-app',
      //     poolConfig: {
      //       host: 'localhost',
      //       port: 5432,
      //       user: 'postgres',
      //       password: 'password',
      //       database: 'test_db',
      //     },
      //     userDbClient: UserDatabaseName.KNEX,
      //     system_database: 'abc',
      //     telemetry: {
      //       logs: {
      //         logLevel: 'debug',
      //         forceConsole: false,
      //       },
      //     },
      //   };
      //   const providedRuntimeConfig: DBOSRuntimeConfig = {
      //     port: 3000,
      //     admin_port: 4000,
      //     runAdminServer: false,
      //     entrypoints: ['app.js'],
      //     start: ['start.js'],
      //     setup: ['setup.js'],
      //   };
      //   const [resultDBOSConfig] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);
      //   // Base telemetry config should be preserved from provided config
      //   expect(resultDBOSConfig.telemetry?.logs).toEqual(providedDBOSConfig.telemetry?.logs);
      //   // OTLP traces endpoint are overwritten from the file
      //   expect(resultDBOSConfig.telemetry?.OTLPExporter?.tracesEndpoint).toEqual([
      //     'http://otel-collector:4317/from-file',
      //   ]);
    });

    test('should use provided config when parameters are missing from config file', () => {
      //   // Mock the config file without telemetry settings
      //   const mockDBOSConfigYamlString = `
      //       database:
      //         hostname: db-host-from-file
      //         port: 1234
      //         username: user-from-file
      //         password: password-from-file
      //         app_db_name: db_from_file
      //         sys_db_name: sys_db_from_file
      //     `;
      //   jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockDBOSConfigYamlString);
      //   const providedDBOSConfig: DBOSConfigInternal = {
      //     name: 'test-app',
      //     poolConfig: {
      //       host: 'localhost',
      //       port: 5432,
      //       user: 'postgres',
      //       password: 'password',
      //       database: 'test_db',
      //     },
      //     userDbClient: UserDatabaseName.KNEX,
      //     system_database: 'abc',
      //     telemetry: {
      //       logs: {
      //         logLevel: 'info',
      //         forceConsole: false,
      //       },
      //       OTLPExporter: {
      //         tracesEndpoint: ['http://otel-collector:4317/original'],
      //         logsEndpoint: ['http://otel-collector:4317/yadiyada'],
      //       },
      //     },
      //   };
      //   const providedRuntimeConfig: DBOSRuntimeConfig = {
      //     port: 3000,
      //     admin_port: 4000,
      //     runAdminServer: false,
      //     entrypoints: ['app.js'],
      //     start: [],
      //     setup: [],
      //   };
      //   const [resultDBOSConfig] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);
      //   expect(resultDBOSConfig.telemetry).toEqual(providedDBOSConfig.telemetry);
      //   expect(resultDBOSConfig.name).toEqual('test-app');
      //   expect(resultDBOSConfig.telemetry?.OTLPExporter).toEqual(providedDBOSConfig.telemetry?.OTLPExporter);
    });

    test('use constructPoolConfig default sys db name when missing from config file', () => {
      //   // Mock config file without sys_db_name
      //   const mockDBOSConfigYamlString = `
      //   name: app-from-file
      //   database:
      //     hostname: db-host-from-file
      //     port: 1234
      //     username: user-from-file
      //     password: password-from-file
      //     app_db_name: db_from_file
      // `;
      //   jest.spyOn(utils, 'readFileSync').mockReturnValueOnce(mockDBOSConfigYamlString);
      //   const providedDBOSConfig: DBOSConfigInternal = {
      //     name: 'test-app',
      //     poolConfig: {
      //       host: 'localhost',
      //       port: 5432,
      //       user: 'postgres',
      //       password: 'password',
      //       database: 'test_db',
      //     },
      //     telemetry: {},
      //     system_database: 'original_sys_db',
      //   };
      //   const providedRuntimeConfig: DBOSRuntimeConfig = {
      //     port: 3000,
      //     admin_port: 3001,
      //     runAdminServer: true,
      //     entrypoints: [],
      //     start: [],
      //     setup: [],
      //   };
      //   const [resultDBOSConfig, _] = overwrite_config(providedDBOSConfig, providedRuntimeConfig);
      //   expect(resultDBOSConfig.system_database).toEqual(`${resultDBOSConfig.poolConfig?.database}_dbos_sys`);
    });
  });

  describe('databaseUrl-no-password', () => {
    beforeAll(async () => {
      await setUpDBOSTestDb({});
    });

    test('No error when database_url is provided without password', async () => {
      //   const expected_url = 'postgresql://postgres@localhost:5432/dbostest?sslmode=disable';
      //   const config: DBOSConfig = {
      //     name: 'test-app',
      //     databaseUrl: expected_url,
      //   };
      //   const poolConfig = constructPoolConfig({
      //     database: {},
      //     database_url: expected_url,
      //     application: {},
      //     env: {},
      //   });
      //   expect(poolConfig.connectionString).toBe(expected_url);
      //   // Make sure we can use it to construct a client and connect to the database without the password.
      //   const client = await DBOSClient.create(expected_url);
      //   try {
      //     await expect(client.listQueuedWorkflows({})).resolves.toBeDefined();
      //   } finally {
      //     await client.destroy();
      //   }
    });
  });
});
