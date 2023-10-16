/* eslint-disable */

import * as utils from "../../src/utils";
import { PoolConfig } from "pg";
import { parseConfigFile } from "../../src/operon-runtime/config";
import { OperonRuntimeConfig } from "../../src/operon-runtime/runtime";
import { OperonConfigKeyTypeError, OperonInitializationError } from "../../src/error";
import { Operon, OperonConfig } from "../../src/operon";
import { WorkflowContextImpl } from "../../src/workflow";

describe("operon-config", () => {
  const mockCLIOptions = { port: NaN, loglevel: "info" };
  const mockOperonConfigYamlString = `
      database:
        hostname: 'some host'
        port: 1234
        username: 'some user'
        password: \${PGPASSWORD}
        connectionTimeoutMillis: 3000
        user_database: 'some DB'
      runtimeConfig:
        port: 1234
        entrypoint: fake-entrypoint
      application:
        payments_url: 'http://somedomain.com/payment'
        foo: \${FOO}
        bar: \$BAR
        nested:
            baz: \$BAZ
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
      jest.spyOn(utils, "readFileSync").mockReturnValue(mockOperonConfigYamlString);

      const [operonConfig, runtimeConfig]: [OperonConfig, OperonRuntimeConfig] = parseConfigFile(mockCLIOptions);

      // Test pool config options
      const poolConfig: PoolConfig = operonConfig.poolConfig;
      expect(poolConfig.host).toBe("some host");
      expect(poolConfig.port).toBe(1234);
      expect(poolConfig.user).toBe("some user");
      expect(poolConfig.password).toBe(process.env.PGPASSWORD);
      expect(poolConfig.connectionTimeoutMillis).toBe(3000);
      expect(poolConfig.database).toBe("some DB");

      // Application config
      const applicationConfig: any = operonConfig.application;
      expect(applicationConfig.payments_url).toBe("http://somedomain.com/payment");
      expect(applicationConfig.foo).toBe(process.env.FOO);
      expect(applicationConfig.bar).toBe(process.env.BAR);
      expect(applicationConfig.nested.baz).toBe(process.env.BAZ);
      expect(applicationConfig.nested.a).toBeInstanceOf(Array);
      expect(applicationConfig.nested.a).toHaveLength(3);
      expect(applicationConfig.nested.a[2].b.c).toBe(process.env.C);

      // local runtime config
      expect(runtimeConfig).toBeDefined();
      expect(runtimeConfig?.port).toBe(1234);
      expect(runtimeConfig?.entrypoint).toBe("fake-entrypoint");
    });

    test("fails to read config file", () => {
      jest.spyOn(utils, "readFileSync").mockImplementation(() => {
        throw new OperonInitializationError("some error");
      });
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(OperonInitializationError);
    });

    test("config file is empty", () => {
      const mockConfigFile = "";
      jest.spyOn(utils, "readFileSync").mockReturnValue(JSON.stringify(mockConfigFile));
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(OperonInitializationError);
    });

    test("config file is missing database config", () => {
      const mockConfigFile = { someOtherConfig: "some other config" };
      jest.spyOn(utils, "readFileSync").mockReturnValue(JSON.stringify(mockConfigFile));
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(OperonInitializationError);
    });

    test("config file is missing database password", () => {
      const dbPassword = process.env.PGPASSWORD;
      delete process.env.PGPASSWORD;
      jest.spyOn(utils, "readFileSync").mockReturnValueOnce(mockOperonConfigYamlString);
      jest.spyOn(utils, "readFileSync").mockReturnValueOnce("SQL STATEMENTS");
      expect(() => parseConfigFile(mockCLIOptions)).toThrow(OperonInitializationError);
      process.env.PGPASSWORD = dbPassword;
    });
  });

  describe("context getConfig()", () => {
    beforeEach(() => {
      jest.spyOn(utils, "readFileSync").mockReturnValue(mockOperonConfigYamlString);
    });

    afterEach(() => {
      jest.restoreAllMocks();
    });

    test("getConfig returns the expected values", async () => {
      const [operonConfig, _operonRuntimeConfig]: [OperonConfig, OperonRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const operon = new Operon(operonConfig);
      const ctx: WorkflowContextImpl = new WorkflowContextImpl(operon, undefined, "testUUID", {}, "testContext");
      // Config key exists
      expect(ctx.getConfig<string>("payments_url")).toBe("http://somedomain.com/payment");
      // Config key does not exist, no default value
      expect(ctx.getConfig<string>("no_key")).toBeUndefined();
      // Config key does not exist, default value
      expect(ctx.getConfig<string>("no_key", "default")).toBe("default");
      // We didn't init, so do some manual cleanup only
      clearInterval(operon.flushBufferID);
      await operon.telemetryCollector.destroy();
    });

    test("getConfig returns the default value when no application config is provided", async () => {
      const localMockOperonConfigYamlString = `
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: \${PGPASSWORD}
          connectionTimeoutMillis: 3000
          user_database: 'some DB'
      `;
      jest.restoreAllMocks();
      jest.spyOn(utils, "readFileSync").mockReturnValue(localMockOperonConfigYamlString);
      const [operonConfig, _operonRuntimeConfig]: [OperonConfig, OperonRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const operon = new Operon(operonConfig);
      const ctx: WorkflowContextImpl = new WorkflowContextImpl(operon, undefined, "testUUID", {}, "testContext");
      expect(ctx.getConfig<string>("payments_url", "default")).toBe("default");
      // We didn't init, so do some manual cleanup only
      clearInterval(operon.flushBufferID);
      await operon.telemetryCollector.destroy();
    });

    test("getConfig throws when it finds a value of different type than the default", async () => {
      const [operonConfig, _operonRuntimeConfig]: [OperonConfig, OperonRuntimeConfig] = parseConfigFile(mockCLIOptions);
      const operon = new Operon(operonConfig);
      const ctx: WorkflowContextImpl = new WorkflowContextImpl(operon, undefined, "testUUID", {}, "testContext");
      expect(() => ctx.getConfig<number>("payments_url", 1234)).toThrow(OperonConfigKeyTypeError);
      // We didn't init, so do some manual cleanup only
      clearInterval(operon.flushBufferID);
      await operon.telemetryCollector.destroy();
    });
  });
});
