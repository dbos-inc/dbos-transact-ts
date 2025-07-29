import * as utils from '../../src/utils';
import {
  ConfigFile,
  getDatabaseUrl,
  getDbosConfig,
  getSystemDatabaseUrl,
  overwriteConfigForDBOSCloud,
  readConfigFile,
  translateDbosConfig,
} from '../../src/dbos-runtime/config';
import { AssertionError } from 'assert';
import { DBOSConfigInternal } from '../../src/dbos-executor';
import { DBOSRuntimeConfig } from '../../src';
import { UserDatabaseName } from '../../src/user_database';
import { TelemetryCollector } from '../../src/telemetry/collector';

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

    test('throws on an invalid config', () => {
      const mockConfigFile = `
      name: 'test-app'
      datafffbase_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable'`;
      jest.spyOn(utils, 'readFileSync').mockReturnValue(mockConfigFile);
      expect(() => readConfigFile()).toThrow();
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

    test.each(['pg-node', 'prisma', 'typeorm', 'knex', 'drizzle'])(
      'handles app_db_client %s',
      (app_db_client: string) => {
        const configFile: ConfigFile = {
          name: 'test-app',
          language: 'node',
          database: {
            app_db_client: app_db_client as UserDatabaseName,
            migrate: ['npx knex migrate:latest'],
          },
          telemetry: {
            logs: {
              logLevel: 'debug',
              addContextMetadata: true,
            },
          },
        };

        const config = getDbosConfig(configFile);
        expect(config.userDbClient).toBe(app_db_client);
      },
    );
  });

  describe('getDatabaseUrl', () => {
    // Question: dbos-config schema disallows 'dbos' as the database.username field
    //           Should we be validating that in database_url?

    test('uses database_url from config when provided', () => {
      const databaseUrl = getDatabaseUrl({
        name: 'Test App',
        database_url: 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable',
      });
      expect(databaseUrl).toBe('postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable');
    });

    test('uses default value even if environment variable is set', () => {
      process.env.DBOS_DATABASE_URL = 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable';
      const databaseUrl = getDatabaseUrl({
        name: 'Test App',
      });
      expect(databaseUrl).toBe('postgresql://postgres:dbos@localhost:5432/test_app?connect_timeout=10&sslmode=disable');
    });

    test('uses environment variable when provided', () => {
      process.env.DBOS_DATABASE_URL = 'postgresql://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable';
      const databaseUrl = getDatabaseUrl({
        name: 'Test App',
        database_url: process.env.DBOS_DATABASE_URL,
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

    test('correctly handles db url w/o password', () => {
      const url = getDatabaseUrl({ database_url: 'postgresql://postgres@localhost:5432/dbostest?sslmode=disable' });
      expect(url).toBe('postgresql://postgres@localhost:5432/dbostest?sslmode=disable');
    });

    test('throws with invalid database_url format', () => {
      expect(() => getDatabaseUrl({ database_url: 'not-a-valid-url' })).toThrow();
    });

    test.each(['postgres://host:5432/db', 'postgres://user:pass@:5432/db', 'postgres://user:pass@host:5432/'])(
      'throws when database_url is missing required fields %s',
      (database_url) => {
        expect(() => getDatabaseUrl({ database_url })).toThrow();
      },
    );

    test.each(['some_DB', '123db', 'very_very_very_long_very_very_very_long_very_very__database_name', 'largeDB'])(
      'throws on invalid database name %s',
      (name) => {
        expect(() => getDatabaseUrl({ database_url: `postgres://host:5432/${name}` })).toThrow();
      },
    );
  });

  describe('getSystemDatabaseUrl', () => {
    test('get from config', () => {
      const url = getSystemDatabaseUrl({ system_database_url: 'postgres://a:b@c:1234/appdb_dbos_sys' });
      expect(url).toBe('postgres://a:b@c:1234/appdb_dbos_sys');
    });

    test('use default url even when env var set', () => {
      process.env.DBOS_SYSTEM_DATABASE_URL = 'postgres://a:b@c:1234/appdb_dbos_sys';
      const url = getSystemDatabaseUrl({ name: 'appdb' });
      expect(url).toBe('postgresql://postgres:dbos@localhost:5432/appdb_dbos_sys?connect_timeout=10&sslmode=disable');
    });

    test('get from user db url', () => {
      const url = getSystemDatabaseUrl('postgres://a:b@c:1234/appdb?connect_timeout=22&sslmode=disable');
      expect(url).toBe('postgres://a:b@c:1234/appdb_dbos_sys?connect_timeout=22&sslmode=disable');
    });

    test('get from default url', () => {
      const url = getSystemDatabaseUrl({ name: 'appdb' });
      expect(url).toBe('postgresql://postgres:dbos@localhost:5432/appdb_dbos_sys?connect_timeout=10&sslmode=disable');
    });

    test('uses PG env values when config is empty', () => {
      process.env.PGHOST = 'envhost';
      process.env.PGPORT = '7777';
      process.env.PGUSER = 'envuser';
      process.env.PGPASSWORD = 'envpass';

      const databaseUrl = getSystemDatabaseUrl({
        name: 'Test App',
      });
      expect(databaseUrl).toBe(
        'postgresql://envuser:envpass@envhost:7777/test_app_dbos_sys?connect_timeout=10&sslmode=allow',
      );
    });
  });

  describe('translateDbosConfig', () => {
    test('translate with name', () => {
      const internalConfig = translateDbosConfig({
        name: 'dbostest',
      });
      expect(internalConfig).toEqual({
        name: 'dbostest',
        databaseUrl: 'postgresql://postgres:dbos@localhost:5432/dbostest?connect_timeout=10&sslmode=disable',
        userDbPoolSize: undefined,
        systemDatabaseUrl:
          'postgresql://postgres:dbos@localhost:5432/dbostest_dbos_sys?connect_timeout=10&sslmode=disable',
        sysDbPoolSize: undefined,
        userDbClient: undefined,
        telemetry: {
          logs: {
            logLevel: 'info',
            addContextMetadata: undefined,
            forceConsole: false,
          },
          OTLPExporter: {
            tracesEndpoint: undefined,
            logsEndpoint: undefined,
          },
        },
      });
    });

    test('translate with force console', () => {
      const internalConfig = translateDbosConfig(
        {
          name: 'dbostest',
        },
        true,
      );
      expect(internalConfig).toEqual({
        name: 'dbostest',
        databaseUrl: 'postgresql://postgres:dbos@localhost:5432/dbostest?connect_timeout=10&sslmode=disable',
        userDbPoolSize: undefined,
        systemDatabaseUrl:
          'postgresql://postgres:dbos@localhost:5432/dbostest_dbos_sys?connect_timeout=10&sslmode=disable',
        sysDbPoolSize: undefined,
        userDbClient: undefined,
        telemetry: {
          logs: {
            logLevel: 'info',
            addContextMetadata: undefined,
            forceConsole: true,
          },
          OTLPExporter: {
            tracesEndpoint: undefined,
            logsEndpoint: undefined,
          },
        },
      });
    });

    test('translate with db url', () => {
      const internalConfig = translateDbosConfig({
        databaseUrl: 'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
      });
      expect(internalConfig).toEqual({
        name: undefined,
        databaseUrl: 'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
        userDbPoolSize: undefined,
        systemDatabaseUrl:
          'postgres://jon:doe@mother:2345/dbostest_dbos_sys?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
        sysDbPoolSize: undefined,
        userDbClient: undefined,
        telemetry: {
          logs: {
            logLevel: 'info',
            addContextMetadata: undefined,
            forceConsole: false,
          },
          OTLPExporter: {
            tracesEndpoint: undefined,
            logsEndpoint: undefined,
          },
        },
      });
    });

    test('translate with db & sysdb urls', () => {
      const internalConfig = translateDbosConfig({
        databaseUrl: 'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
        systemDatabaseUrl: 'postgres://foo:bar@father:1234/blahblahblah',
      });
      expect(internalConfig).toEqual({
        name: undefined,
        databaseUrl: 'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
        userDbPoolSize: undefined,
        systemDatabaseUrl: 'postgres://foo:bar@father:1234/blahblahblah',
        sysDbPoolSize: undefined,
        userDbClient: undefined,
        telemetry: {
          logs: {
            logLevel: 'info',
            addContextMetadata: undefined,
            forceConsole: false,
          },
          OTLPExporter: {
            tracesEndpoint: undefined,
            logsEndpoint: undefined,
          },
        },
      });
    });
  });

  describe('overwriteConfigForDBOSCloud', () => {
    const internalConfig: DBOSConfigInternal = {
      databaseUrl: 'postgres://jon:doe@mother:2345/dbostest?sslmode=require&sslrootcert=my_cert&connect_timeout=7',
      name: 'my-app',
      systemDatabaseUrl: 'postgres://foo:bar@father:1234/blahblahblah',
      telemetry: {
        logs: {
          logLevel: 'info',
          forceConsole: false,
        },
        OTLPExporter: {
          tracesEndpoint: ['http://otel-collector:4317/traces'],
          logsEndpoint: ['http://otel-collector:4317/logs'],
        },
      },
    };
    const runtimeConfig: DBOSRuntimeConfig = {
      port: 0,
      admin_port: 0,
      runAdminServer: false,
      start: [],
      setup: [],
    };

    test('throws when cloud db url is missing', () => {
      expect(() => overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, {})).toThrow();
    });

    test('uses cloud app name', () => {
      process.env.DBOS_DATABASE_URL = 'fake://url';
      const [newConfig] = overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, { name: 'cloud-app-name' });
      expect(newConfig.name).toBe('cloud-app-name');
    });

    test('uses cloud db url', () => {
      process.env.DBOS_DATABASE_URL = 'postgres://a:b@c:2345/cloud_db';
      const [newConfig] = overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, { name: 'cloud-app-name' });
      expect(newConfig.databaseUrl).toBe('postgres://a:b@c:2345/cloud_db');
    });

    test('uses cloud sys db url when set', () => {
      process.env.DBOS_DATABASE_URL = 'fake://url';
      process.env.DBOS_SYSTEM_DATABASE_URL = 'postgres://a:b@c:2345/cloud_sys_db';
      const [newConfig] = overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, { name: 'cloud-app-name' });
      expect(newConfig.systemDatabaseUrl).toBe('postgres://a:b@c:2345/cloud_sys_db');
    });

    test('derives cloud sys db url from cloud db url', () => {
      process.env.DBOS_DATABASE_URL = 'postgres://a:b@c:2345/cloud_db';
      const [newConfig] = overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, { name: 'cloud-app-name' });
      expect(newConfig.systemDatabaseUrl).toBe('postgres://a:b@c:2345/cloud_db_dbos_sys');
    });

    test('force admin server', () => {
      process.env.DBOS_DATABASE_URL = 'fake://url';
      const [, newRuntimeConfig] = overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, {});
      expect(newRuntimeConfig.admin_port).toBe(3001);
      expect(newRuntimeConfig.runAdminServer).toBe(true);
    });

    test('combine otel endpoints', () => {
      console.log(internalConfig.telemetry.OTLPExporter);
      process.env.DBOS_DATABASE_URL = 'fake://url';
      const [newConfig] = overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, {
        telemetry: {
          OTLPExporter: {
            tracesEndpoint: ['http://otel-collector:4317/traces-from-cloud'],
            logsEndpoint: ['http://otel-collector:4317/logs-from-cloud'],
          },
        },
      });
      expect(newConfig.telemetry.OTLPExporter?.logsEndpoint).toEqual([
        'http://otel-collector:4317/logs',
        'http://otel-collector:4317/logs-from-cloud',
      ]);
      expect(newConfig.telemetry.OTLPExporter?.tracesEndpoint).toEqual([
        'http://otel-collector:4317/traces',
        'http://otel-collector:4317/traces-from-cloud',
      ]);
      console.log(internalConfig.telemetry.OTLPExporter);
    });

    test('combine otel endpoints no duplicates', () => {
      console.log(internalConfig.telemetry.OTLPExporter);
      process.env.DBOS_DATABASE_URL = 'fake://url';
      const [newConfig] = overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, {
        telemetry: {
          OTLPExporter: {
            tracesEndpoint: ['http://otel-collector:4317/traces'],
            logsEndpoint: ['http://otel-collector:4317/logs'],
          },
        },
      });
      expect(newConfig.telemetry.OTLPExporter?.logsEndpoint).toEqual(['http://otel-collector:4317/logs']);
      expect(newConfig.telemetry.OTLPExporter?.tracesEndpoint).toEqual(['http://otel-collector:4317/traces']);
      console.log(internalConfig.telemetry.OTLPExporter);
    });
  });
});
