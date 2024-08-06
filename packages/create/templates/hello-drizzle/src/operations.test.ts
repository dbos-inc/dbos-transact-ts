import { TestingRuntime, createTestingRuntime } from "@dbos-inc/dbos-sdk";
import { Hello } from "./operations";
import request from "supertest";

describe("operations-test", () => {
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    testRuntime = await createTestingRuntime();
  });

  afterAll(async () => {
    await testRuntime.destroy();
  });

  /**
   * Test the transaction.
   */
  test("test-transaction", async () => {
    const res = await testRuntime.invoke(Hello).helloTransaction("dbos");
    expect(res).toMatch("Hello, dbos! We have made");

    // Check the greet count.
    const rows = await testRuntime.queryUserDB("SELECT * FROM dbos_hello WHERE greet_count=1");
    expect(rows.length).toEqual(1);
  });

  /**
   * Test the HTTP endpoint.
   */
  test("test-endpoint", async () => {
    const res = await request(testRuntime.getHandlersCallback()).get(
      "/greeting/dbos"
    );
    expect(res.statusCode).toBe(200);
    expect(res.text).toMatch("Hello, dbos! We have made");
  });
});
