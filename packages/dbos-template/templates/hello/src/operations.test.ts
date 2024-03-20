import { TestingRuntime, createTestingRuntime } from "@dbos-inc/dbos-sdk";
import { Hello, dbos_hello } from "./operations";
import request from "supertest";

describe("operations-test", () => {
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    testRuntime = await createTestingRuntime([Hello]);
  });

  afterAll(async () => {
    await testRuntime.destroy();
  });

  /**
   * Test the transaction.
   */
  test("test-transaction", async () => {
    const res = await testRuntime.invoke(Hello).helloTransaction("dbos");
    expect(res).toMatch("Hello, dbos! You have been greeted");

    // Check the greet count.
    const rows = await testRuntime.queryUserDB<dbos_hello>("SELECT * FROM dbos_hello WHERE name=$1", "dbos");
    expect(rows[0].greet_count).toBe(1);
  });

  /**
   * Test the HTTP endpoint.
   */
  test("test-endpoint", async () => {
    const res = await request(testRuntime.getHandlersCallback()).get(
      "/greeting/dbos"
    );
    expect(res.statusCode).toBe(200);
    expect(res.text).toMatch("Hello, dbos! You have been greeted");
  });
});
