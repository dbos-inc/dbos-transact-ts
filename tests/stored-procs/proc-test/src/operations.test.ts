import { TestingRuntime, createTestingRuntime } from "@dbos-inc/dbos-sdk";
import { StoredProcTest } from "./operations";
import { v1 as uuidv1 } from "uuid";

import { transaction_outputs } from "../../../../schemas/user_db_schema";

describe("operations-test", () => {
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    testRuntime = await createTestingRuntime([StoredProcTest]);
    await testRuntime.queryUserDB('TRUNCATE dbos_hello');
  });

  afterAll(async () => {
    await testRuntime?.destroy();
  });

  test("test-procGreetingWorkflow", async () => {
    const wfid = uuidv1();
    const user = "procWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfid).procGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);
    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[1].error);
  });

  test("test-txGreetingWorkflow", async () => {
    const wfid = uuidv1();

    const user = "txWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfid).txAndProcGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);
    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[1].error);
  });

  test("test-procLocalGreetingWorkflow", async () => {
    const wfid = uuidv1();
    const user = "procLocalWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfid).txAndProcGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);
    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[1].error);
  });
  
  test("test-procErrorWorkflow", async () => {
    const wfid = uuidv1();
    const user = "procErrorWF";
    await expect(testRuntime.invokeWorkflow(StoredProcTest, wfid).procErrorWorkflow(user)).rejects.toThrow("This is a test error");

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);

    expect(txRows.length).toBe(3);

    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`1`);
    expectNullResult(txRows[1].error);

    expect(txRows[2].function_id).toBe(2);
    expectNullResult(txRows[2].output);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(JSON.parse(txRows[2].error).message).toMatch("This is a test error");
  })
});


function expectNullResult(result: string | null) {
  if (typeof result === 'string') {
    expect(JSON.parse(result)).toBeNull();
  } else {
    expect(result).toBeNull();
  }
}