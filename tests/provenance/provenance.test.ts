import { generateOperonTestConfig, setupOperonTestDb } from "../helpers";
import { ProvenanceDaemon } from "../../src/provenance/provenance_daemon";
import { POSTGRES_EXPORTER } from "../../src/telemetry";
import { OperonTransaction, OperonWorkflow } from "../../src/decorators";
import { Operon, OperonConfig, TransactionContext, WorkflowContext } from "../../src";

describe("operon-provenance", () => {
  const testTableName = "operon_test_kv";

  let operon: Operon;
  let config: OperonConfig;
  let provDaemon: ProvenanceDaemon;

  beforeAll(async () => {
    config = generateOperonTestConfig([POSTGRES_EXPORTER]);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    operon = new Operon(config);
    operon.useNodePostgres();
    operon.registerDecoratedWT();
    await operon.init();
    operon.registerTopic("testTopic", ["defaultRole"]);
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`
    );
    provDaemon = new ProvenanceDaemon(config, "jest_test_slot");
    await provDaemon.start();
  });

  afterEach(async () => {
    await operon.destroy();
    await provDaemon.stop();
  });

  class TestFunctions {
    @OperonTransaction()
    static async testFunction(ctxt: TransactionContext, name: string) {
      await ctxt.pgClient.query(`INSERT INTO ${testTableName}(value) VALUES ($1)`, [name]);
    };

    @OperonWorkflow()
    static async testWorkflow(ctxt: WorkflowContext, name: string) {
      await ctxt.transaction(TestFunctions.testFunction, name);
    }
  }

  test("basic-provenance", async () => {
    await operon.workflow(TestFunctions.testWorkflow, {}, "write one").getResult();
    await provDaemon.recordProvenance();
  });
});
