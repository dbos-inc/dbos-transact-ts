import { TestingRuntime, parseConfigFile } from "@dbos-inc/dbos-sdk";
import { StoredProcTest } from "./operations";
import { v1 as uuidv1 } from "uuid";

import { transaction_outputs } from "../../../schemas/user_db_schema";
import { createInternalTestRuntime } from "@dbos-inc/dbos-sdk/dist/src/testing/testing_runtime";
import { DBOSConfig } from "@dbos-inc/dbos-sdk";

describe("operations-test", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    [config,] = parseConfigFile();
    testRuntime = await createInternalTestRuntime([StoredProcTest], config)
    await testRuntime.queryUserDB('TRUNCATE dbos_hello');
  });

  afterAll(async () => {
    await testRuntime?.destroy();
  });

  test("test-procGreetingWorkflow", async () => {
    const wfUUID = uuidv1();
    const user = "procWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfUUID).procGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfUUID);
    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[1].error);
  });

  test("test-debug-procGreetingWorkflow", async () => {
    const wfUUID = uuidv1();
    const user = "debugProcWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfUUID).procGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const debugConfig = { ...config, debugMode: true };
    const debugRuntime = await createInternalTestRuntime([StoredProcTest], debugConfig);
    try {
      expect(debugRuntime).toBeDefined();
      const debugRes = await debugRuntime.invokeWorkflow(StoredProcTest, wfUUID).procGreetingWorkflow(user);
      expect(debugRes).toEqual(res);
    } finally {
      await debugRuntime.destroy();
    }


    // throw new Error(JSON.stringify(debugConfig, null, 4));

    // await expect((debugRuntime as TestingRuntimeImpl).getDBOSExec().executeWorkflowUUID(wfUUID).then((x) => x.getResult())).resolves.toBe(1);




    // const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfid);
    // expect(txRows.length).toBe(2);
    // expect(txRows[0].function_id).toBe(0);
    // expect(txRows[0].output).toBe("0");
    // expectNullResult(txRows[0].error);

    // expect(txRows[1].function_id).toBe(1);
    // expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    // expectNullResult(txRows[1].error);
  });

  test("test-txGreetingWorkflow", async () => {
    const wfUUID = uuidv1();

    const user = "txWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfUUID).txAndProcGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfUUID);
    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[1].error);
  });

  test("test-procLocalGreetingWorkflow", async () => {
    const wfUUID = uuidv1();
    const user = "procLocalWF";
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfUUID).txAndProcGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfUUID);
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