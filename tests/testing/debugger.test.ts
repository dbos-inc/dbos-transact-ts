import { WorkflowContext, TransactionContext, Transaction, Workflow, DBOSInitializer, InitContext, CommunicatorContext, Communicator, Debug } from "../../src/";
import { generateDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from "../helpers";
import { v1 as uuidv1 } from "uuid";
import { DBOSConfig } from "../../src/dbos-executor";
import { PoolClient } from "pg";
import { TestingRuntime, createInternalTestRuntime } from "../../src/testing/testing_runtime";

type TestTransactionContext = TransactionContext<PoolClient>;
const testTableName = "debugger_test_kv";

describe("debugger-test", () => {
  let username: string;
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;
  let testRuntime: TestingRuntime;
  let debugRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    debugConfig = generateDBOSTestConfig(undefined, "http://127.0.0.1:5432");
    username = config.poolConfig.user || "postgres";
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    // TODO: connect to the real proxy.
    debugRuntime = await createInternalTestRuntime([DebuggerTest], debugConfig);
    testRuntime = await createInternalTestRuntime([DebuggerTest], config);
  });

  afterEach(async () => {
    await debugRuntime.destroy();
    await testRuntime.destroy();
  });

  class DebuggerTest {
    static cnt: number = 0;

    @DBOSInitializer()
    static async init(ctx: InitContext) {
      await ctx.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
      await ctx.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    }

    @Transaction()
    static async testFunction(txnCtxt: TestTransactionContext, name: string) {
      const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`, [name]);
      return Number(rows[0].id);
    }

    @Workflow()
    static async testWorkflow(ctxt: WorkflowContext, name: string) {
      const funcResult = await ctxt.invoke(DebuggerTest).testFunction(name);
      return funcResult;
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @Communicator()
    static async testCommunicator(_ctxt: CommunicatorContext) {
      return ++DebuggerTest.cnt;
    }

    @Workflow()
    static async receiveWorkflow(ctxt: WorkflowContext, debug?: boolean) {
      const message1 = await ctxt.recv<string>();
      const message2 = await ctxt.recv<string>();
      const fail = await ctxt.recv("fail", 0);
      if (debug) {
        await ctxt.recv("shouldn't happen", 0);
      }
      return message1 === "message1" && message2 === "message2" && fail === null;
    }

    @Workflow()
    static async sendWorkflow(ctxt: WorkflowContext, destinationUUID: string) {
      await ctxt.send(destinationUUID, "message1");
      await ctxt.send(destinationUUID, "message2");
    }
  }

  test("debug-workflow", async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    const res = await testRuntime
      .invoke(DebuggerTest, wfUUID)
      .testWorkflow(username)
      .then((x) => x.getResult());
    expect(res).toBe(1);
    await testRuntime.destroy();

    // Execute again in debug mode.
    const debugRes = await debugRuntime
      .invoke(DebuggerTest, wfUUID)
      .testWorkflow(username)
      .then((x) => x.getResult());
    expect(debugRes).toBe(1);

    // Execute a non-exist UUID should fail.
    const wfUUID2 = uuidv1();
    const nonExist = await debugRuntime.invoke(DebuggerTest, wfUUID2).testWorkflow(username);
    await expect(nonExist.getResult()).rejects.toThrow("Workflow status not found!");

    // Execute a workflow without specifying the UUID should fail.
    await expect(debugRuntime.invoke(DebuggerTest).testWorkflow(username)).rejects.toThrow("Workflow UUID not found!");
  });

  test("debug-transaction", async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    await expect(testRuntime.invoke(DebuggerTest, wfUUID).testFunction(username)).resolves.toBe(1);
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID).testFunction(username)).resolves.toBe(1);

    // Execute a non-exist UUID should fail.
    const wfUUID2 = uuidv1();
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID2).testFunction(username)).rejects.toThrow("This should never happen during debug.");

    // Execute a workflow without specifying the UUID should fail.
    await expect(debugRuntime.invoke(DebuggerTest).testFunction(username)).rejects.toThrow("Workflow UUID not found!");
  });

  test("debug-communicator", async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    await expect(testRuntime.invoke(DebuggerTest, wfUUID).testCommunicator()).resolves.toBe(1);
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID).testCommunicator()).resolves.toBe(1);

    // Execute a non-exist UUID should fail.
    const wfUUID2 = uuidv1();
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID2).testCommunicator()).rejects.toThrow("Cannot find recorded communicator");

    // Execute a workflow without specifying the UUID should fail.
    await expect(debugRuntime.invoke(DebuggerTest).testCommunicator()).rejects.toThrow("Workflow UUID not found!");
  });

  test("debug-workflow-notifications", async() => {
    const recvUUID = uuidv1();
    const sendUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    const handle = await testRuntime.invoke(DebuggerTest, recvUUID).receiveWorkflow(false);
    await expect(testRuntime.invoke(DebuggerTest, sendUUID).sendWorkflow(recvUUID).then((x) => x.getResult())).resolves.toBeFalsy(); // return void.
    expect(await handle.getResult()).toBe(true);
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invoke(DebuggerTest, recvUUID).receiveWorkflow(false).then((x) => x.getResult())).resolves.toBe(true);
    await expect(debugRuntime.invoke(DebuggerTest, sendUUID).sendWorkflow(recvUUID).then((x) => x.getResult())).resolves.toBeFalsy();

    // Execute a non-exist UUID should fail.
    const wfUUID2 = uuidv1();
    await expect(debugRuntime.send(recvUUID, "testmsg", "testtopic", wfUUID2)).rejects.toThrow("Cannot find recorded send");
    await expect(debugRuntime.invoke(DebuggerTest, recvUUID).receiveWorkflow(true).then((x) => x.getResult())).rejects.toThrow("Cannot find recorded recv");
  });
});
