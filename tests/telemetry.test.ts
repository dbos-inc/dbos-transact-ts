import {
  ConsoleExporter,
  TelemetrySignal,
  PostgresExporter,
  POSTGRES_EXPORTER,
  TelemetryCollector,
} from "../src/telemetry";
import { Operon } from "../src/operon";

import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { observabilityDBSchema } from "schemas/observability_db_schema";
import { QueryConfig } from "pg";

describe("operon-telemetry", () => {
  test("Only configures requested exporters", async () => {
    const collector = new TelemetryCollector([new ConsoleExporter()]);
    expect(collector.exporters.length).toBe(1);
    await collector.destroy();
  });

  describe("Postgres exporter", () => {
    let operon: Operon;

    beforeEach(async () => {
      const operonConfig = generateOperonTestConfig([POSTGRES_EXPORTER]);
      await setupOperonTestDb(operonConfig);
      operon = new Operon(operonConfig);
    });

    afterEach(async () => {
      await operon.destroy();
    });

    test("Configures and initializes", async () => {
      // First check that the Telemetry Collector is properly initialized with a valid PostgresExporter
      const collector = operon.telemetryCollector;
      expect(collector.exporters.length).toBe(1);
      expect(collector.exporters[0]).toBeInstanceOf(PostgresExporter);
      const pgExporter: PostgresExporter = collector
        .exporters[0] as PostgresExporter;

      // Then check PostgresExporter initialization
      const loadSchemaSpy = jest.spyOn(pgExporter.pgClient, "query");
      await collector.init();
      expect(loadSchemaSpy).toHaveBeenCalledWith(observabilityDBSchema);

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
      // XXX this is a hack: the test will now have to expose registered operations for the collector init() to insert the tables
      const signal1: TelemetrySignal = {
          workflowName: "test",
          workflowUUID: "test",
          functionName: "create_user",
          functionID: 0,
          runAs: "test",
          timestamp: Date.now(),
          severity: "INFO",
          log_message: "test",
      };
      const signal2 = { ...signal1 } ;
      signal2.log_message = "test2";
      collector.push(signal1);
      collector.push(signal2);
      await collector.processAndExportSignals();

      const pgExporter = collector.exporters[0] as PostgresExporter;
      const pgExporterPgClient = pgExporter.pgClient;
      const queryResult =
        await pgExporterPgClient.query<TelemetrySignal>(
          `select * from signal_create_user` // XXX hacked table name
        );
      expect(queryResult.rows).toHaveLength(2);
      expect(queryResult.rows[0].log_message).toBe("test");
      expect(queryResult.rows[1].log_message).toBe("test2");

      // Clean up the database XXX we need a test database
      const cleanUpQuery: QueryConfig = {
        text: "delete from log_signal where workflow_uuid=$1 or workflow_uuid=$2",
        values: [
          queryResult.rows[0].workflowUUID,
          queryResult.rows[1].workflowUUID,
        ],
      };
      await pgExporterPgClient.query(cleanUpQuery);
    });
  });
});
