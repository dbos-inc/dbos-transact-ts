import {
  generateOperonTestConfig,
  setupOperonTestDb,
} from "../helpers";
import { Operon, OperonConfig, TransactionContext } from "../../src";
import { ProvenanceDaemon } from "../../src/provenance/provenance_daemon";
import { CONSOLE_EXPORTER } from "src/telemetry";

describe("operon-provenance", () => {
  const testTableName = "operon_test_kv";

  let operon: Operon;
  let config: OperonConfig;
  let provDaemon: ProvenanceDaemon;

  beforeAll(async () => {
    config = generateOperonTestConfig([CONSOLE_EXPORTER]);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    operon = new Operon(config);
    operon.useNodePostgres();
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

  test("basic-provenance", async () => {
    const testFunction = async (ctxt: TransactionContext, name: string) => {
      await ctxt.pgClient.query(
        `INSERT INTO ${testTableName}(value) VALUES ($1)`,
        [name]
      );
    };
    operon.registerTransaction(testFunction);
    await operon.transaction(testFunction, {}, "write one");
    await operon.transaction(testFunction, {}, "write two");
    await operon.transaction(testFunction, {}, "write three");
    await provDaemon.recordProvenance();
  });
});
