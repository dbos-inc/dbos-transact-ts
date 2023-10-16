import { OperonTestingRuntime, createTestingRuntime } from "@dbos-inc/operon";
import { Hello, operon_hello } from "./operations";
import request from "supertest";

describe("operations-test", () => {
  let testRuntime: OperonTestingRuntime;

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
    const res = await testRuntime.invoke(Hello).helloTransaction("operon");
    expect(res).toMatch("Hello, operon! You have been greeted");

    // Check the greet count.
    const rows = await testRuntime.queryUserDB<operon_hello>("SELECT * FROM operon_hello WHERE name=$1", "operon");
    expect(rows[0].greet_count).toBe(1);
  });

  /**
   * Test the HTTP endpoint.
   */
  test("test-endpoint", async () => {
    const res = await request(testRuntime.getHandlersCallback()).get(
      "/greeting/operon"
    );
    expect(res.statusCode).toBe(200);
    expect(res.text).toMatch("Hello, operon! You have been greeted");
  });
});
