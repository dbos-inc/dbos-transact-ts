/*
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { ProvenanceDaemon } from "../../src/provenance/provenance_daemon";
// import { PostgresExporter } from "../../src/telemetry/exporters";
import { DBOSTransaction, DBOSWorkflow } from "../../src/decorators";
import { TestingRuntime, TransactionContext, WorkflowContext } from "../../src";
import { PgTransactionId } from "../../src/workflow";
import { DBOSConfig } from "../../src/dbos-sdk";
import { PoolClient } from "pg";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";

describe("dbos-provenance", () => {
  const testTableName = "dbos_test_kv";

  let config: DBOSConfig;
  let provDaemon: ProvenanceDaemon;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime([TestFunctions], config);
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
    @DBOSTransaction()
    static async testTransaction(ctxt: TransactionContext<PoolClient>, name: string) {
      await ctxt.client.query(`INSERT INTO ${testTableName}(value) VALUES ($1)`, [name]);
      return (await ctxt.client.query<PgTransactionId>("select CAST(pg_current_xact_id() AS TEXT) as txid;")).rows[0].txid;
    }

    @DBOSWorkflow()
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

    const wfe = (testRuntime as TestingRuntimeImpl).getWFE();
    const pgExporter = wfe.telemetryCollector.exporters[1] as PostgresExporter;
    let { rows } = await pgExporter.pgClient.query(`SELECT * FROM provenance_logs WHERE transaction_id=$1`, [xid]);
    expect(rows.length).toBeGreaterThan(0);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(rows[0].table_name).toBe(testTableName);
    await wfe.telemetryCollector.processAndExportSignals();
    ({ rows } = await pgExporter.pgClient.query(`SELECT * FROM signal_testtransaction WHERE transaction_id=$1`, [xid]));
    expect(rows.length).toBeGreaterThan(0);
  });
});
*/
