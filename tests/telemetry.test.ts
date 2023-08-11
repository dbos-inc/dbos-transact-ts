import {
  ConsoleExporter,
  TelemetrySignal,
  PostgresExporter,
  POSTGRES_EXPORTER,
  TelemetryCollector,
  CONSOLE_EXPORTER,
} from "../src/telemetry";
import { Operon, OperonConfig } from "../src/operon";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { logged } from "../src/decorators";
import {
  TransactionContext,
  WorkflowConfig,
  WorkflowContext,
  WorkflowParams,
} from "src";
import { WorkflowHandle } from "src/workflow";

type TelemetrySignalDbFields = {
  workflow_uuid: string;
  function_id: number;
  function_name: string;
  run_as: string;
  timestamp: bigint;
  severity: string;
  log_message: string;
};

class TestClass {
  @logged
  static create_user(name: string): Promise<string> {
    return Promise.resolve(name);
  }

  @logged
  static async test_function(
    txnCtxt: TransactionContext,
    name: string
  ): Promise<string> {
    const { rows } = await txnCtxt.client.query(
      `select current_user from current_user where current_user=$1;`,
      [name]
    );
    const result = JSON.stringify(rows[0]);
    txnCtxt.log("INFO", `transaction result: ${result}`);
    return result;
  }

  @logged
  static async test_workflow(
    workflowCtxt: WorkflowContext,
    name: string
  ): Promise<string> {
    const funcResult: string = await workflowCtxt.transaction(
      TestClass.test_function,
      name
    );
    workflowCtxt.log("INFO", `workflow result: ${funcResult}`);
    return funcResult;
  }
}

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

  describe("Console exporter", () => {
    let operon: Operon;
    const operonConfig = generateOperonTestConfig([CONSOLE_EXPORTER]);
    let collector: TelemetryCollector;

    beforeEach(() => {
      operon = new Operon(operonConfig);
    });

    afterEach(async () => {
      await collector.destroy();
      await operon.destroy();
    });

    test("console.log is called with the correct messages", async () => {
      collector = operon.telemetryCollector;
      expect(collector.exporters.length).toBe(1);
      expect(collector.exporters[0]).toBeInstanceOf(ConsoleExporter);

      await collector.init();
      const logSpy = jest.spyOn(global.console, "log");

      const signal1: TelemetrySignal = {
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
      expect(logSpy).toHaveBeenCalledTimes(2);
      expect(logSpy).toHaveBeenNthCalledWith(
        1,
        `[${signal1.severity}] ${signal1.logMessage}`
      );
      expect(logSpy).toHaveBeenNthCalledWith(
        2,
        `[${signal1.severity}] ${signal2.logMessage}`
      );
    });
  });

  describe("Postgres exporter", () => {
    let operon: Operon;
    let operonConfig: OperonConfig;
    beforeAll(async () => {
      operonConfig = generateOperonTestConfig([POSTGRES_EXPORTER]);
      operon = new Operon(operonConfig);
      await operon.init();
    });

    afterAll(async () => {
      await operon.destroy();
    });

    test("single workflow single operation", async () => {
      operon.registerTransaction(TestClass.test_function);
      const testWorkflowConfig: WorkflowConfig = {
        rolesThatCanRun: ["operonAppAdmin", "operonAppUser"],
      };
      operon.registerWorkflow(TestClass.test_workflow, testWorkflowConfig);
      const params: WorkflowParams = {
        runAs: "operonAppAdmin",
      };
      const username = operonConfig.poolConfig.user as string;
      const workflowHandle: WorkflowHandle<string> = operon.workflow(
        TestClass.test_workflow,
        params,
        username
      );
      const workflowUUID = workflowHandle.getWorkflowUUID();
      const result: string = await workflowHandle.getResult();

      // Workflow should have executed correctly
      expect(JSON.parse(result)).toEqual({ current_user: username });

      // Exporter should have registered the correct tables
      expect(operon.telemetryCollector.exporters.length).toBe(1);
      expect(operon.telemetryCollector.exporters[0]).toBeInstanceOf(
        PostgresExporter
      );
      const pgExporter = operon.telemetryCollector
        .exporters[0] as PostgresExporter;
      const pgExporterPgClient = pgExporter.pgClient;
      const tableQueryResult = await pgExporterPgClient.query(
        `SELECT tablename FROM pg_catalog.pg_tables where tablename like 'signal_%'`
      );
      expect(tableQueryResult.rows).toHaveLength(3);
      expect(tableQueryResult.rows).toContainEqual({
        tablename: "signal_test_function",
      });
      expect(tableQueryResult.rows).toContainEqual({
        tablename: "signal_test_workflow",
      });

      // Exporter should export the log entries
      await operon.telemetryCollector.processAndExportSignals();

      const txnLogQueryResult =
        await pgExporterPgClient.query<TelemetrySignalDbFields>(
          `SELECT * FROM signal_test_function`
        );
      expect(txnLogQueryResult.rows).toHaveLength(1);
      const txnLogEntry = txnLogQueryResult.rows[0];
      expect(txnLogEntry.workflow_uuid).toBe(workflowUUID);
      expect(txnLogEntry.function_id).toBe(1);
      expect(txnLogEntry.function_name).toBe("test_function");
      expect(txnLogEntry.run_as).toBe(params.runAs);
      expect(txnLogEntry.severity).toBe("INFO");
      expect(txnLogEntry.log_message).toBe(`transaction result: ${result}`);

      const wfLogQueryResult =
        await pgExporterPgClient.query<TelemetrySignalDbFields>(
          `SELECT * FROM signal_test_workflow`
        );
      expect(wfLogQueryResult.rows).toHaveLength(1);
      const wfLogEntry = wfLogQueryResult.rows[0];
      expect(wfLogEntry.workflow_uuid).toBe(workflowUUID);
      expect(wfLogEntry.function_id).toBe(2);
      expect(wfLogEntry.function_name).toBe("test_workflow");
      expect(wfLogEntry.run_as).toBe(params.runAs);
      expect(wfLogEntry.severity).toBe("INFO");
      expect(wfLogEntry.log_message).toBe(`workflow result: ${result}`);
    });
  });
});
