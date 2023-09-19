import {
    Operon,
    OperonConfig,
    WorkflowContext,
    TransactionContext,
    CommunicatorContext,
    WorkflowParams,
    WorkflowHandle,
    OperonCommunicator,
    OperonWorkflow,
    OperonTransaction,
  } from "src/";
  import {
    generateOperonTestConfig,
    setupOperonTestDb,
    TestKvTable,
  } from "./helpers";
  import { v1 as uuidv1 } from "uuid";
  import { sleep } from "src/utils";
  import { WorkflowConfig, StatusString } from "src/workflow";
  import { CONSOLE_EXPORTER } from "src/telemetry";

  const testTableName = "operon_test_kv";

  class TestLogic {

    @OperonTransaction({ readOnly: true })
    static async testReadTx(ctx: TransactionContext, id: number) {
        const { rows } = await ctx.pgClient.query<TestKvTable>(
            `SELECT value FROM ${testTableName} WHERE id=$1`,
            [id]
          );

        return rows.length > 0 ? rows[0].value : undefined;
    }

    @OperonTransaction()
    static async testWriteTx(ctx: TransactionContext, value: string) {
        const { rows } = await ctx.pgClient.query<TestKvTable>(
          `INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`,
          [value.toUpperCase()]
        );
        return Number(rows[0].id);
    }

    @OperonWorkflow()
    static async testWorkflow(ctx: WorkflowContext, value: string) {
      const id = await ctx.transaction(TestLogic.testWriteTx, value);
      const $value = await ctx.transaction(TestLogic.testReadTx, id);
      return { id, value: $value, originalValue: value }
    }
  }


  describe("decorator tests", () => {
    test("TestLogic", async () => {
        const config = generateOperonTestConfig([CONSOLE_EXPORTER]);
        await setupOperonTestDb(config);
        const operon = new Operon(config);
        operon.useNodePostgres();

        await operon.init(TestLogic);

        await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
        await operon.userDatabase.query(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);

        const workflowUUID = uuidv1();
        const result = await operon
          .workflow(TestLogic.testWorkflow, { workflowUUID }, "foo")
          .getResult();
        expect(result.originalValue).toBe("foo");
        expect(result.value).toBe("FOO");
  
        await operon.destroy();
    });

  })