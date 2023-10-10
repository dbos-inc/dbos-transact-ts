import { OperonTestingRuntime, createTestingRuntime } from "@dbos-inc/operon";
import { Hello } from "./userFunctions";
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
   * Test the tansaction.
   */
  test("test-transaction", async () => {
    const res = await testRuntime.invoke(Hello).helloTransaction("operon");
    expect(res).toMatch("Hello, operon! You have been greeted");
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
