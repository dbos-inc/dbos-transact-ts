import { DBOS } from "@dbos-inc/dbos-sdk";
import { Hello, dbos_hello } from "./operations";
//import request from "supertest";

describe("operations-test", () => {
  beforeAll(async () => {
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  /**
   * Test the transaction.
   */
  test("test-transaction", async () => {
    const res = await Hello.helloTransaction("dbos");
    expect(res).toMatch("Hello, dbos! You have been greeted");

    // Check the greet count.
    const rows = await DBOS.executor.queryUserDB("SELECT * FROM dbos_hello WHERE name=$1", ["dbos"]) as dbos_hello[];
    expect(rows[0].greet_count).toBe(1);
  });
});
