import {
  ConsoleExporter,
  TelemetrySignal,
  PostgresExporter,
  POSTGRES_EXPORTER,
  TelemetryCollector,
  CONSOLE_EXPORTER,
} from "../src/telemetry";
import { Operon, OperonConfig } from "../src/operon";

import { generateOperonTestConfig, teardownOperonTestDb } from "./helpers";
import { observabilityDBSchema } from "schemas/observability_db_schema";
import { Client } from "pg";

type TelemetrySignalDbFields = {
  workflow_name: string;
  workflow_uuid: string;
  function_id: number;
  function_name: string;
  run_as: string;
  timestamp: bigint;
  severity: string;
  log_message: string;
};

describe("operon-telemetry", () => {
  test("Operon init works with all exporters", async () => {
    const operonConfig = generateOperonTestConfig([
      CONSOLE_EXPORTER,
      POSTGRES_EXPORTER,
    ]);
    const operon = new Operon(operonConfig);
    await operon.init();
    await operon.destroy();
  });

  describe("Postgres exporter", () => {
    let operon: Operon;
    const operonConfig = generateOperonTestConfig([POSTGRES_EXPORTER]);
    let collector: TelemetryCollector;

    beforeEach(() => {
      operon = new Operon(operonConfig);
    });

    afterEach(async () => {
      await collector.destroy();
      await operon.destroy();
      await teardownOperonTestDb(operonConfig);
    });

    test("Configures and initializes", async () => {
      // First check that the Telemetry Collector is properly initialized with a valid PostgresExporter
      collector = operon.telemetryCollector;
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

      await collector.destroy();
    });

    test("Signals are correctly exported", async () => {
      collector = operon.telemetryCollector;
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
        logMessage: "test",
      };
      const signal2 = { ...signal1 };
      signal2.logMessage = "test2";
      collector.push(signal1);
      collector.push(signal2);
      await collector.processAndExportSignals();

      const pgExporter = collector.exporters[0] as PostgresExporter;
      const pgExporterPgClient = pgExporter.pgClient;
      const queryResult =
        await pgExporterPgClient.query<TelemetrySignalDbFields>(
          `select * from signal_create_user` // XXX hacked table name
        );
      expect(queryResult.rows).toHaveLength(2);
      expect(queryResult.rows[0].log_message).toBe("test");
      expect(queryResult.rows[1].log_message).toBe("test2");
    });
  });
});
