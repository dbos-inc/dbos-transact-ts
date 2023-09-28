import { Operon, OperonConfig, OperonInitializationError } from "src/";
import { generateOperonTestConfig } from "./helpers";
import * as utils from "../src/utils";
import { PoolConfig } from "pg";

describe("operon-config", () => {
  const mockOperonConfigYamlString = `
      database:
        hostname: 'some host'
        port: 1234
        username: 'some user'
        connectionTimeoutMillis: 3000
        user_database: 'some DB'

      httpServer:
        port: 3000  

      application:
        payments_url: 'http://somedomain.com/payment'  
      `;

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test("Config is valid and is parsed as expected", async () => {
    jest
      .spyOn(utils, "readFileSync")
      .mockReturnValueOnce(mockOperonConfigYamlString);
    jest.spyOn(utils, "readFileSync").mockReturnValueOnce("SQL STATEMENTS");

    const operon: Operon = new Operon();
    expect(operon.initialized).toBe(false);
    const operonConfig: OperonConfig = operon.config;

    // Test pool config options
    const poolConfig: PoolConfig = operonConfig.poolConfig;
    expect(poolConfig.host).toBe("some host");
    expect(poolConfig.port).toBe(1234);
    expect(poolConfig.user).toBe("some user");
    expect(poolConfig.password).toBe(process.env.PGPASSWORD);
    expect(poolConfig.connectionTimeoutMillis).toBe(3000);
    expect(poolConfig.database).toBe("some DB");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(operonConfig.application.payments_url).toBe("http://somedomain.com/payment")
    await operon.destroy();
  });

  test("Custom config is parsed as expected", async () => {
    // We use readFileSync as a proxy for checking the config was not read from the default location
    const readFileSpy = jest.spyOn(utils, "readFileSync");
    const config: OperonConfig = generateOperonTestConfig();
    const operon = new Operon(config);
    expect(operon.initialized).toBe(false);
    expect(operon.config).toBe(config);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(operon.config.application.counter).toBe(3);
    expect(readFileSpy).toHaveBeenCalledTimes(0);
    await operon.destroy();
  });

  test("fails to read config file", () => {
    jest.spyOn(utils, "readFileSync").mockImplementation(() => {
      throw new Error("some error");
    });
    expect(() => new Operon()).toThrow(OperonInitializationError);
  });

  test("config file is empty", () => {
    const mockConfigFile = "";
    jest
      .spyOn(utils, "readFileSync")
      .mockReturnValue(JSON.stringify(mockConfigFile));
    expect(() => new Operon()).toThrow(OperonInitializationError);
  });

  test("config file is missing database config", () => {
    const mockConfigFile = { someOtherConfig: "some other config" };
    jest
      .spyOn(utils, "readFileSync")
      .mockReturnValue(JSON.stringify(mockConfigFile));
    expect(() => new Operon()).toThrow(OperonInitializationError);
  });
});
