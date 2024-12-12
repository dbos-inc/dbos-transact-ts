import { DBOS, TestingRuntime, parseConfigFile } from "@dbos-inc/dbos-sdk";
import { StoredProcTest } from "./operations";
import { v1 as uuidv1 } from "uuid";

import { transaction_outputs } from "../../../schemas/user_db_schema";
import { workflow_status } from "../../../schemas/system_db_schema";
import { TestingRuntimeImpl, createInternalTestRuntime } from "@dbos-inc/dbos-sdk/dist/src/testing/testing_runtime";
import { DBOSConfig } from "@dbos-inc/dbos-sdk";
import { Client, ClientConfig } from "pg";

async function runSql<T>(config: ClientConfig, func: (client: Client) => Promise<T>) {
  const client = new Client(config);
  try {
    await client.connect();
    return await func(client);
  } finally {
    await client.end();
  }
}

function randomString(length?: number) {
  length ??= 4 + Math.floor(Math.random() * 10);

  let result = '';
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * characters.length));
  }
  return result;
}



describe("operations-test", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    [config,] = parseConfigFile();
    testRuntime = await createInternalTestRuntime([StoredProcTest], config)

    await testRuntime.queryUserDB(`
      DROP ROUTINE IF EXISTS "StoredProcTest_getGreetCountLocal_p";
      DROP ROUTINE IF EXISTS "StoredProcTest_getGreetCountLocal_f";
      DROP ROUTINE IF EXISTS "StoredProcTest_helloProcedureLocal_p"; 
      DROP ROUTINE IF EXISTS "StoredProcTest_helloProcedureLocal_f";`);
  });

  afterAll(async () => {
    await testRuntime?.destroy();
  });

  test("test-procReplay", async () => {
    const now = Date.now();
    const user = `procReplay-${now}`;
    const res = await testRuntime.invoke(StoredProcTest).helloProcedure(user);
    expect(res).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const wfUUID = `procReplay-wfid-${now}`;
    expect(await testRuntime.invoke(StoredProcTest, wfUUID).getGreetCount(user)).toBe(1);

    await testRuntime.retrieveWorkflow(wfUUID).getResult();

    const statuses = await runSql({ ...config.poolConfig, database: config.system_database }, async (client) => {
      const { rows } = await client.query<workflow_status>("SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1", [wfUUID]);
      return rows;
    });

    expect(statuses.length).toBe(1);
    expect(statuses[0].status).toBe("SUCCESS");
    expect(statuses[0].output).toBe(JSON.stringify(1));
    expectNullResult(statuses[0].error);

    expect(await testRuntime.invoke(StoredProcTest, wfUUID).getGreetCount(user)).toBe(1);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfUUID);
    expect(txRows.length).toBe(1);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("1");
    expectNullResult(txRows[0].error);
  });

  test("test-procGreetingWorkflow", async () => {
    const wfUUID = uuidv1();
    const user = `procWF_${randomString()}`;
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfUUID).procGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expect(res.rowCount).toBeGreaterThan(0);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfUUID);
    expect(txRows.length).toBe(3);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[1].error);

    expect(txRows[2].function_id).toBe(2);
    expect(txRows[2].output).toEqual(`${res.rowCount}`);
    expectNullResult(txRows[1].error);
  });

  test("test-debug-procGreetingWorkflow", async () => {
    const wfUUID = uuidv1();
    const user = `debugProcWF_${randomString()}`;
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfUUID).procGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expect(res.rowCount).toBeGreaterThan(0);

    const debugConfig = { ...config, debugMode: true };
    const debugRuntime = await createInternalTestRuntime([StoredProcTest], debugConfig);
    try {
      expect(debugRuntime).toBeDefined();
      await expect(debugRuntime.invokeWorkflow(StoredProcTest, wfUUID).procGreetingWorkflow(user)).resolves.toEqual(res);
      await expect((debugRuntime as TestingRuntimeImpl).getDBOSExec().executeWorkflowUUID(wfUUID).then((x) => x.getResult())).resolves.toEqual(res);
    } finally {
      await debugRuntime.destroy();
    }
  });

  test("test-txGreetingWorkflow", async () => {
    const wfUUID = uuidv1();

    const user = `txWF_${randomString()}`;
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
    const user = `procLocalWF_${randomString()}`;    
    const res = await testRuntime.invokeWorkflow(StoredProcTest, wfUUID).procLocalGreetingWorkflow(user);
    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expect(res.rowCount).toBeGreaterThan(0);

    const txRows = await testRuntime.queryUserDB<transaction_outputs>("SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1", wfUUID);
    expect(txRows.length).toBe(3);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe("0");
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[1].error);

    expect(txRows[2].function_id).toBe(2);
    expect(txRows[2].output).toEqual(`${res.rowCount}`);
    expectNullResult(txRows[1].error);
  });

  test("test-txAndProcGreetingWorkflow", async () => {
    const wfUUID = uuidv1();
    const user = `txAndProcWF_${randomString()}`;
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

  test("test-txAndProcGreetingWorkflow_v2", async () => {

    DBOS.setConfig(config);
    await DBOS.launch();

    try {

      const wfUUID = uuidv1();
      const user = `txAndProcWFv2_${randomString()}`;
      const res = await DBOS.withNextWorkflowID(wfUUID, async () => {
        return await StoredProcTest.txAndProcGreetingWorkflow_v2(user);
      })

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
    } finally {
      await DBOS.shutdown();
    }
  });


  test("test-procErrorWorkflow", async () => {
    const wfid = uuidv1();
    const user = `procErrorWF_${randomString()}`;
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