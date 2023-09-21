import { CommunicatorContext, Operon, OperonCommunicator, OperonConfig, OperonWorkflow, WorkflowContext } from "src";
import { CONSOLE_EXPORTER } from "src/telemetry";
import { sleep } from "src/utils";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { v1 as uuidv1 } from "uuid";

class TestClass {
  static #counter = 0;
  static get counter() { return TestClass.#counter; }
  @OperonCommunicator()
  static async testCommunicator(commCtxt: CommunicatorContext) {
    void commCtxt;
    await sleep(1);
    return TestClass.#counter++;
  }

  @OperonWorkflow()
  static async testWorkflow(workflowCtxt: WorkflowContext) {
    const funcResult = await workflowCtxt.external(TestClass.testCommunicator);
    return funcResult ?? -1;
  }
}

describe("decorator-tests", () => {

  const testTableName = "operon_test_kv";

  let operon: Operon;
  let username: string;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig([CONSOLE_EXPORTER]);
    username = config.poolConfig.user || "postgres";
    await setupOperonTestDb(config);
  })


  beforeEach(async () => {
    operon = new Operon(config);
    operon.useNodePostgres();
    await operon.init(TestClass);
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`
    );
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("simple-communicator-decorator", async () => {

    const workflowUUID: string = uuidv1();
    const initialCounter = TestClass.counter;

    let result: number = await operon
      .workflow(TestClass.testWorkflow, { workflowUUID: workflowUUID })
      .getResult();
    expect(result).toBe(initialCounter);
    expect(TestClass.counter).toBe(initialCounter + 1);

    // Test OAOO. Should return the original result.
    result = await operon
      .workflow(TestClass.testWorkflow, { workflowUUID: workflowUUID })
      .getResult();
    expect(result).toBe(initialCounter);
    expect(TestClass.counter).toBe(initialCounter + 1);
  })

});
