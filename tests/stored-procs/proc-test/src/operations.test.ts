import { TestingRuntime, createTestingRuntime } from "@dbos-inc/dbos-sdk";
import { StoredProcTest, dbos_hello } from "./operations";
import { v5 as uuidv5 } from "uuid";
import { transaction_outputs } from "../../../../schemas/user_db_schema";

describe("operations-test", () => {
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    testRuntime = await createTestingRuntime([StoredProcTest]);
  });

  afterAll(async () => {
    await testRuntime.destroy();
  });

  test("test-procGreetingWorkflow", async () => {
    const wfid = uuidv5("procWF", uuidv5.URL)
    const handle = await testRuntime.invoke(StoredProcTest, wfid).procGreetingWorkflow("procWF");
    const res = await handle.getResult();
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch("Hello, procWF! You have been greeted 1 times.");

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);
    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    expect(txRows[0].error).toBeNull();

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch("Hello, procWF! You have been greeted 1 times.");
    expect(txRows[1].error).toBeNull();
  });

  test("test-procLocalGreetingWorkflow", async () => {
    const wfid = uuidv5("procLocalWF", uuidv5.URL)
    const handle = await testRuntime.invoke(StoredProcTest, wfid).procLocalGreetingWorkflow("procLocalWF");
    const res = await handle.getResult();
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch("Hello, procLocalWF! You have been greeted 1 times.");

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);
    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    expect(txRows[0].error).toBeNull();

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch("Hello, procLocalWF! You have been greeted 1 times.");
    expect(txRows[1].error).toBeNull();
  });
  
});
