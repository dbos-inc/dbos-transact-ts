import {
  ConsoleExporter,
  PostgresExporter,
  POSTGRES_EXPORTER,
  TelemetryCollector,
} from "../src/telemetry";
import { Operon } from "../src/operon";

import { generateOperonTestConfig } from "./helpers";
import postgresLogBackendSchema from "schemas/postgresLogBackend";
import { sleep } from "src/utils";

describe("operon-telemetry", () => {
  test("Only configures requested exporters", async () => {
    let collector = new TelemetryCollector([new ConsoleExporter()]);
    expect(collector.exporters.length).toBe(1);
    await collector.destroy();
  });

  test("Signal queue is functional", async () => {
    // Note we don't test pop() because it is a private method
    const collector = new TelemetryCollector([]);
    expect(() => collector.push("a")).not.toThrow();
    expect(() => collector.push("b")).not.toThrow();
    await collector.destroy();
  });

  describe("Postgres exporter", () => {
    let operon: Operon;

    beforeEach(async () => {
      const operonConfig = generateOperonTestConfig([POSTGRES_EXPORTER]);
      operon = new Operon(operonConfig);
      // Operon PG system's client is normally connected during operon.init()
      await operon.pgSystemClient.connect();
    });

    afterEach(async () => {
      await operon.destroy();
      await operon.pgSystemClient.end();
    });

    test("Configures and initializes", async () => {
      // First check that the Telemetry Collector is properly initialized with a valid PostgresExporter
      const collector = operon.telemetryCollector;
      expect(collector.exporters.length).toBe(1);
      expect(collector.exporters[0]).toBeInstanceOf(PostgresExporter);
      const pgExporter = collector.exporters[0] as PostgresExporter;

      // Then check PostgresExporter initialization
      const dbCheckSpy = jest.spyOn(operon.pgSystemClient, "query");
      const loadSchemaSpy = jest.spyOn(pgExporter.pgClient, "query");
      await collector.init();
      expect(dbCheckSpy).toHaveBeenCalledWith(
        "SELECT FROM pg_database WHERE datname = 'pglogsbackend'"
      );
      expect(loadSchemaSpy).toHaveBeenCalledWith(postgresLogBackendSchema);

      // Check the exporter's PG client is functional
      const queryResult = await pgExporter.pgClient.query(
        `select current_user from current_user`
      );
      expect(queryResult.rows).toHaveLength(1);
    });

    test("Signals are correctly exported", async () => {
      const collector = operon.telemetryCollector;
      await collector.init();

      // Push to the signals queue and wait for one export interval
      collector.push("a");
      collector.push("b");
      await sleep(1000); // Set to TelemetryCollector.processAndExportSignalsIntervalMs

      const pgExporter = collector.exporters[0] as PostgresExporter;
      const pgExporterPgClient = pgExporter.pgClient;
      const queryResult = await pgExporterPgClient.query(
        `select * from log_signal`
      );
      expect(queryResult.rows).toHaveLength(2);
      expect(queryResult.rows[0].log_signal_raw).toBe("a");
      expect(queryResult.rows[1].log_signal_raw).toBe("b");

      // Clean up the database XXX we need a test database
      const workflowInstanceIds = queryResult.rows.map(
        (row) => `'${row.workflow_instance_id}'`
      );
      const function_ids = queryResult.rows.map(
        (row) => `'${row.function_id}'`
      );

      const cleanUpQuery = `delete from log_signal where workflow_instance_id in (${workflowInstanceIds}) and function_id in (${function_ids})`;
      await pgExporterPgClient.query(cleanUpQuery);
    });
  });
});
