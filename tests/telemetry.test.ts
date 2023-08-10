import {
  ConsoleExporter,
  TelemetrySignalLog,
  PostgresExporter,
  POSTGRES_EXPORTER,
  TelemetryCollector,
} from "../src/telemetry";
import { Operon } from "../src/operon";

import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import postgresLogBackendSchema from "schemas/postgresLogBackend";
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
      await collector.processAndExportSignals();

      const pgExporter = collector.exporters[0] as PostgresExporter;
      const pgExporterPgClient = pgExporter.pgClient;
      const queryResult =
        await pgExporterPgClient.query<TelemetrySignalLog>(
          `select * from log_signal`
        );
      expect(queryResult.rows).toHaveLength(2);
      expect(queryResult.rows[0].log_signal_raw).toBe("a");
      expect(queryResult.rows[1].log_signal_raw).toBe("b");

      // Clean up the database XXX we need a test database
      const cleanUpQuery: QueryConfig = {
        text: "delete from log_signal where workflow_uuid=$1 or workflow_uuid=$2",
        values: [
          queryResult.rows[0].workflow_uuid,
          queryResult.rows[1].workflow_uuid,
        ],
      };
      await pgExporterPgClient.query(cleanUpQuery);
    });
  });
});
