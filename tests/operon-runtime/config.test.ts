/* eslint-disable */

import * as utils from "../../src/utils";
import { PoolConfig } from "pg";
import { parseConfigFile } from "../../src/operon-runtime/config";
import { OperonRuntimeConfig } from "../../src/operon-runtime/runtime";
import { OperonInitializationError } from "../../src/error";
import { OperonConfig } from "../../src/operon";

describe("operon-config", () => {
  const mockCLIOptions = {port: NaN, loglevel: "info"};
  const mockOperonConfigYamlString = `
      database:
        hostname: 'some host'
        port: 1234
        username: 'some user'
        password: \${PGPASSWORD}
        connectionTimeoutMillis: 3000
        user_database: 'some DB'
      localRuntimeConfig:
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

  test("Config is valid and is parsed as expected", () => {
    jest.spyOn(utils, "readFileSync").mockReturnValueOnce(mockOperonConfigYamlString);
    jest.spyOn(utils, "readFileSync").mockReturnValueOnce("SQL STATEMENTS");

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
    expect(applicationConfig.foo).toBe("foo");
    expect(applicationConfig.bar).toBe("bar");
    expect(applicationConfig.nested.baz).toBe("baz");
    expect(applicationConfig.nested.a).toBeInstanceOf(Array);
    expect(applicationConfig.nested.a).toHaveLength(3);
    expect(applicationConfig.nested.a[2].b.c).toBe("c");

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
    delete process.env.PGPASSWORD;
    jest.spyOn(utils, "readFileSync").mockReturnValueOnce(mockOperonConfigYamlString);
    jest.spyOn(utils, "readFileSync").mockReturnValueOnce("SQL STATEMENTS");
    expect(() => parseConfigFile(mockCLIOptions)).toThrow(OperonInitializationError);
  });
});
