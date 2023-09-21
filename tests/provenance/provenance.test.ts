import { generateOperonTestConfig, setupOperonTestDb } from "../helpers";
import { ProvenanceDaemon } from "../../src/provenance/provenance_daemon";
import { POSTGRES_EXPORTER, PostgresExporter } from "../../src/telemetry";
import { OperonTransaction, OperonWorkflow } from "../../src/decorators";
import { Operon, OperonConfig, TransactionContext, WorkflowContext } from "../../src";
import { PgTransactionId } from "../../src/workflow";

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
    await operon.init(TestFunctions);
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
    static async testTransaction(ctxt: TransactionContext, name: string) {
      await ctxt.pgClient.query(`INSERT INTO ${testTableName}(value) VALUES ($1)`, [name]);
      return (
        await ctxt.pgClient.query<PgTransactionId>(
          "select CAST(pg_current_xact_id() AS TEXT) as txid;"
        )
      ).rows[0].txid;
    }

    @OperonWorkflow()
    static async testWorkflow(ctxt: WorkflowContext, name: string) {
      return await ctxt.transaction(TestFunctions.testTransaction, name);
    }
  }

  test("basic-provenance", async () => {
    const xid: string = await operon.workflow(TestFunctions.testWorkflow, {}, "write one").getResult();
    await provDaemon.recordProvenance();
    await provDaemon.telemetryCollector.processAndExportSignals();
    const pgExporter = operon.telemetryCollector
    .exporters[0] as PostgresExporter;
    let { rows } = await pgExporter.pgClient.query(`SELECT * FROM provenance_logs WHERE transaction_id=$1`, [xid]);
    expect(rows.length).toBeGreaterThan(0);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(rows[0].table_name).toBe(testTableName);
    await operon.telemetryCollector.processAndExportSignals();
    ({ rows } = await pgExporter.pgClient.query(`SELECT * FROM signal_testtransaction WHERE transaction_id=$1`, [xid]));
    expect(rows.length).toBeGreaterThan(0);

  });
});
