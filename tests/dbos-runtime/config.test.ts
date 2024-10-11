/* eslint-disable */

import * as utils from "../../src/utils";
import { UserDatabaseName } from "../../src/user_database";
import { PoolConfig } from "pg";
import { parseConfigFile } from "../../src/dbos-runtime/config";
import { DBOSRuntimeConfig, defaultEntryPoint } from "../../src/dbos-runtime/runtime";
import { DBOSConfigKeyTypeError, DBOSInitializationError } from "../../src/error";
import { DBOSExecutor, DBOSConfig } from "../../src/dbos-executor";
import { WorkflowContextImpl } from "../../src/workflow";
import { get } from "lodash";

describe("dbos-config", () => {
  const mockCLIOptions = { port: NaN, loglevel: "info" };
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

  describe("Configuration parsing", () => {
    test("Config is valid and is parsed as expected", () => {
      jest.spyOn(utils, "readFileSync").mockReturnValue(mockDBOSConfigYamlString);

      const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);

      // Test pool config options
      const poolConfig: PoolConfig = dbosConfig.poolConfig;
      expect(poolConfig.host).toBe("some host");
      expect(poolConfig.port).toBe(1234);
      expect(poolConfig.user).toBe("some user");
      expect(poolConfig.password).toBe(process.env.PGPASSWORD);
      expect(poolConfig.connectionTimeoutMillis).toBe(3000);
      expect(poolConfig.database).toBe("some_db");
      expect(poolConfig.ssl).toBe(false);

      expect(dbosConfig.userDbclient).toBe(UserDatabaseName.KNEX);

      // Application config
      const applicationConfig: object = dbosConfig.application || {};
      expect(get(applicationConfig, 'payments_url')).toBe("http://somedomain.com/payment");
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

    test("runtime config correctly parses entrypoints", () => {
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
      jest.spyOn(utils, "readFileSync").mockReturnValue(mockDBOSConfigWithEntryPoints);

      const [_, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);

      expect(runtimeConfig).toBeDefined();
      expect(runtimeConfig?.port).toBe(1234);
      expect(runtimeConfig?.admin_port).toBe(2345);
      expect(runtimeConfig.entrypoints).toBeDefined();
      expect(runtimeConfig.entrypoints).toBeInstanceOf(Array);
      expect(runtimeConfig.entrypoints).toHaveLength(2);
      expect(runtimeConfig.entrypoints[0]).toBe("a");
      expect(runtimeConfig.entrypoints[1]).toBe("b");
    });

    test("fails to read config file", () => {
      jest.spyOn(utils, "readFileSync").mockImplementation(() => {
        throw new DBOSInitializationError("some error");
      });
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test("config file is empty", () => {
      const mockConfigFile = "";
      jest.spyOn(utils, "readFileSync").mockReturnValue(JSON.stringify(mockConfigFile));
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test("config file is missing database config", () => {
      const mockConfigFile = { someOtherConfig: "some other config" };
      jest.spyOn(utils, "readFileSync").mockReturnValue(JSON.stringify(mockConfigFile));
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test("config file is missing database password", () => {
      const dbPassword = process.env.PGPASSWORD;
      delete process.env.PGPASSWORD;
      jest.spyOn(utils, "readFileSync").mockReturnValueOnce(mockDBOSConfigYamlString);
      jest.spyOn(utils, "readFileSync").mockReturnValueOnce("SQL STATEMENTS");
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
      process.env.PGPASSWORD = dbPassword;
    });

    test("config file is missing app database name", () => {
      const localMockDBOSConfigYamlString = `
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
      `;
      jest.spyOn(utils, "readFileSync").mockReturnValueOnce(localMockDBOSConfigYamlString);
      jest.spyOn(utils, "readFileSync").mockReturnValueOnce("SQL STATEMENTS");
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test("config file specifies the wrong language", () => {
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
      jest.spyOn(utils, "readFileSync").mockReturnValueOnce(localMockDBOSConfigYamlString);
      jest.spyOn(utils, "readFileSync").mockReturnValueOnce("SQL STATEMENTS");
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });
  });

  describe("context getConfig()", () => {
    beforeEach(() => {
      jest.spyOn(utils, "readFileSync").mockReturnValue(mockDBOSConfigYamlString);
    });

    afterEach(() => {
      jest.restoreAllMocks();
    });

    test("getConfig returns the expected values", async () => {
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const dbosExec = new DBOSExecutor(dbosConfig);
      const ctx: WorkflowContextImpl = new WorkflowContextImpl(dbosExec, undefined, "testUUID", {}, "testContext", true, undefined, undefined);
      // Config key exists
      expect(ctx.getConfig("payments_url")).toBe("http://somedomain.com/payment");
      // Config key does not exist, no default value
      expect(ctx.getConfig("no_key")).toBeUndefined();
      // Config key does not exist, default value
      expect(ctx.getConfig("no_key", "default")).toBe("default");
      // We didn't init, so do some manual cleanup only
      clearInterval(dbosExec.flushBufferID);
      await dbosExec.telemetryCollector.destroy();
    });

    test("getConfig returns the default value when no application config is provided", async () => {
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
      jest.spyOn(utils, "readFileSync").mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const dbosExec = new DBOSExecutor(dbosConfig);
      const ctx: WorkflowContextImpl = new WorkflowContextImpl(dbosExec, undefined, "testUUID", {}, "testContext", true, undefined, undefined);
      expect(ctx.getConfig<string>("payments_url", "default")).toBe("default");
      // We didn't init, so do some manual cleanup only
      clearInterval(dbosExec.flushBufferID);
      await dbosExec.telemetryCollector.destroy();
    });

    test("environment variables are set correctly", async () => {
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
      jest.spyOn(utils, "readFileSync").mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const dbosExec = new DBOSExecutor(dbosConfig);
      expect(process.env.FOOFOO).toBe("barbar");
      expect(process.env.RANDENV).toBe(""); // Empty string
      // We didn't init, so do some manual cleanup only
      clearInterval(dbosExec.flushBufferID);
      await dbosExec.telemetryCollector.destroy();
    });

    test("ssl true enables ssl", async () => {
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
      jest.spyOn(utils, "readFileSync").mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig.ssl).toEqual({ rejectUnauthorized: false });
    });

    test("ssl defaults off for localhost", async () => {
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
      jest.spyOn(utils, "readFileSync").mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig.ssl).toBe(false);
    });

    test("ssl defaults on for not-localhost", async () => {
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
      jest.spyOn(utils, "readFileSync").mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      expect(dbosConfig.poolConfig.ssl).toEqual({ rejectUnauthorized: false });
    });

    test("getConfig throws when it finds a value of different type than the default", async () => {
      const [dbosConfig, _dbosRuntimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const dbosExec = new DBOSExecutor(dbosConfig);
      const ctx: WorkflowContextImpl = new WorkflowContextImpl(dbosExec, undefined, "testUUID", {}, "testContext", true, undefined, undefined);
      expect(() => ctx.getConfig<number>("payments_url", 1234)).toThrow(DBOSConfigKeyTypeError);
      // We didn't init, so do some manual cleanup only
      clearInterval(dbosExec.flushBufferID);
      await dbosExec.telemetryCollector.destroy();
    });

    test("parseConfigFile throws on an invalid config", async () => {
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
      jest.spyOn(utils, "readFileSync").mockReturnValue(localMockDBOSConfigYamlString);
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test("parseConfigFile disallows the user to be dbos", async () => {
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
      jest.spyOn(utils, "readFileSync").mockReturnValue(localMockDBOSConfigYamlString);
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
    });

    test("parseConfigFile allows undefined password for debug mode with proxy", async () => {
      const dbPassword = process.env.PGPASSWORD;
      delete process.env.PGPASSWORD;
      const localMockDBOSConfigYamlString = `
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          app_db_name: 'some_db'
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, "readFileSync").mockReturnValue(localMockDBOSConfigYamlString);
      const [dbosConfig, _]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile(mockCLIOptions, true); // Use proxy

      // Test pool config options
      const poolConfig: PoolConfig = dbosConfig.poolConfig;
      expect(poolConfig.host).toBe("some host");
      expect(poolConfig.port).toBe(1234);
      expect(poolConfig.user).toBe("some user");
      expect(poolConfig.password).toBe("PROXY-MODE"); // Should be set to "PROXY-MODE"
      expect(poolConfig.database).toBe("some_db");
      process.env.PGPASSWORD = dbPassword;
    });

    test("parseConfigFile throws on an invalid db name", async () => {
      const invalidNames = ["some_DB", "123db", "very_very_very_long_very_very_very_long_very_very__database_name", "largeDB", ""];
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
        jest.spyOn(utils, "readFileSync").mockReturnValue(localMockDBOSConfigYamlString);
        expect(() => parseConfigFile(mockCLIOptions)).toThrow(DBOSInitializationError);
      }
    });
  });
});
