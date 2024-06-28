import { TestingRuntime, createTestingRuntime } from "@dbos-inc/dbos-sdk";
import { StoredProcTest } from "./operations";
import { v5 as uuidv5 } from "uuid";
import { transaction_outputs } from "../../../../schemas/user_db_schema";

describe("operations-test", () => {
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    testRuntime = await createTestingRuntime([StoredProcTest]);
    await testRuntime.queryUserDB('TRUNCATE dbos.transaction_outputs');
    await testRuntime.queryUserDB('TRUNCATE dbos_hello');
  });

  afterAll(async () => {
    await testRuntime.destroy();
  });

  function testNullResult(result: string | null) {
    if (typeof result === 'string') {
      expect(JSON.parse(result)).toBeNull();
    } else {
      expect(result).toBeNull();
    }
  }

  test("test-procGreetingWorkflow", async () => {
    const wfid = uuidv5("test-procGreetingWorkflow", uuidv5.URL)
    const user = "procWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfid).procGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);
    console.log(wfid);
    console.log(txRows);

    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    testNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    testNullResult(txRows[1].error);
  });

  test("test-txGreetingWorkflow", async () => {
    const wfid = uuidv5("test-txGreetingWorkflow", uuidv5.URL)
    const user = "txWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfid).txGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);
    console.log(wfid);
    console.log(txRows);

    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    testNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    testNullResult(txRows[1].error);
  });

  test("test-procLocalGreetingWorkflow", async () => {
    const wfid = uuidv5("test-procLocalGreetingWorkflow", uuidv5.URL)
    const user = "procLocalWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfid).txGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);
    console.log(wfid);
    console.log(txRows);

    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    testNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    testNullResult(txRows[1].error);
  });
  
});
