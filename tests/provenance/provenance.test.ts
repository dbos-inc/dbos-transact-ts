import { generateOperonTestConfig, setupOperonTestDb } from "../helpers";
import { ProvenanceDaemon } from "../../src/provenance/provenance_daemon";
import { POSTGRES_EXPORTER, PostgresExporter } from "../../src/telemetry/exporters";
import { OperonTransaction, OperonWorkflow } from "../../src/decorators";
import { OperonTestingRuntime, TransactionContext, WorkflowContext, createTestingRuntime } from "../../src";
import { PgTransactionId } from "../../src/workflow";
import { OperonConfig } from "../../src/operon";
import { PoolClient } from "pg";
import { OperonTestingRuntimeImpl } from "../../src/testing/testing_runtime";

describe("operon-provenance", () => {
  const testTableName = "operon_test_kv";

  let config: OperonConfig;
  let provDaemon: ProvenanceDaemon;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig([POSTGRES_EXPORTER]);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createTestingRuntime([TestFunctions], config);
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    provDaemon = new ProvenanceDaemon(config, "jest_test_slot");
    await provDaemon.start();
  });

  afterEach(async () => {
    await testRuntime.destroy();
    await provDaemon.stop();
  });

  class TestFunctions {
    @OperonTransaction()
    static async testTransaction(ctxt: TransactionContext<PoolClient>, name: string) {
      await ctxt.client.query(`INSERT INTO ${testTableName}(value) VALUES ($1)`, [name]);
      return (await ctxt.client.query<PgTransactionId>("select CAST(pg_current_xact_id() AS TEXT) as txid;")).rows[0].txid;
    }

    @OperonWorkflow()
    static async testWorkflow(ctxt: WorkflowContext, name: string) {
      return await ctxt.invoke(TestFunctions).testTransaction(name);
    }
  }

  test("basic-provenance", async () => {
    const xid: string = await testRuntime
      .invoke(TestFunctions)
      .testWorkflow("write one")
      .then((x) => x.getResult());
    await provDaemon.recordProvenance();
    await provDaemon.telemetryCollector.processAndExportSignals();

    const operon = testRuntime.getOperon();
    const pgExporter = operon.telemetryCollector.exporters[0] as PostgresExporter;
    let { rows } = await pgExporter.pgClient.query(`SELECT * FROM provenance_logs WHERE transaction_id=$1`, [xid]);
    expect(rows.length).toBeGreaterThan(0);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(rows[0].table_name).toBe(testTableName);
    await operon.telemetryCollector.processAndExportSignals();
    ({ rows } = await pgExporter.pgClient.query(`SELECT * FROM signal_testtransaction WHERE transaction_id=$1`, [xid]));
    expect(rows.length).toBeGreaterThan(0);
  });
});
