import { TestingRuntime, createTestingRuntime } from "@dbos-inc/dbos-sdk";
import { Hello } from "./operations";
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
   * Test the HTTP endpoint.
   */
  test("test-greet", async () => {
    const res = await request(testRuntime.getHandlersCallback()).get(
      "/greeting/dbos"
    );
    expect(res.statusCode).toBe(200);
    expect(res.text).toMatch("Greeting 1: Hello, dbos!");
  });
});
