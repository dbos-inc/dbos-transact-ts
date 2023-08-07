/* eslint-disable @typescript-eslint/no-explicit-any */

import { deserializeError, serializeError } from "serialize-error";
import { operonNull, OperonNull, function_outputs } from "./operon";
import { Pool } from 'pg';

export interface SystemDatabase {
  checkWorkflowOutput<R>(workflowUUID: string) : Promise<OperonNull | R>;
  recordWorkflowOutput<R>(workflowUUID: string, output: R) : Promise<void>;
  recordWorkflowError(workflowUUID: string, error: Error) : Promise<void>;

  checkCommunicatorOutput<R>(workflowUUID: string, functionID: number) : Promise<OperonNull | R>;
  recordCommunicatorOutput<R>(workflowUUID: string, functionID: number, output: R) : Promise<void>;
  recordCommunicatorError(workflowUUID: string, functionID: number, error: Error): Promise<void>;

  send<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic: string, key: string, message: T) : Promise<boolean>;
  recv<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic: string, key: string, timeout: number) : Promise<T | null>;
}

export class PostgresSystemDatabase implements SystemDatabase {

  constructor(readonly pool: Pool) {}
  checkWorkflowOutput<R>(workflowUUID: string): Promise<OperonNull | R> {
    throw new Error("Method not implemented.");
  }

  recordWorkflowOutput<R>(workflowUUID: string, output: R): Promise<void> {
    throw new Error("Method not implemented.");
  }

  recordWorkflowError(workflowUUID: string, error: Error): Promise<void> {
    throw new Error("Method not implemented.");
  }

  async checkCommunicatorOutput<R>(workflowUUID: string, functionID: number): Promise<OperonNull | R> {
    const { rows } = await this.pool.query<function_outputs>("SELECT output, error FROM operon.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2",
      [workflowUUID, functionID]);
    if (rows.length === 0) {
      return operonNull;
    } else if (JSON.parse(rows[0].error) !== null) {
      throw deserializeError(JSON.parse(rows[0].error));
    } else {
      return JSON.parse(rows[0].output) as R;
    }
  }

  async recordCommunicatorOutput<R>(workflowUUID: string, functionID: number, output: R): Promise<void> {
    const serialOutput = JSON.stringify(output);
    await this.pool.query("INSERT INTO operon.operation_outputs (workflow_uuid, function_id, output, error) VALUES ($1, $2, $3, $4);",
      [workflowUUID, functionID, serialOutput, null]);
  }

  async recordCommunicatorError(workflowUUID: string, functionID: number, error: Error): Promise<void> {
    const serialErr = JSON.stringify(serializeError(error));
    await this.pool.query("INSERT INTO operon.operation_outputs (workflow_uuid, function_id, output, error) VALUES ($1, $2, $3, $4);",
    [workflowUUID, functionID, null, serialErr]);
  }

  send<T extends unknown>(workflowUUID: string, functionID: number, topic: string, key: string, message: T): Promise<boolean> {
    throw new Error("Method not implemented.");
  }

  recv<T extends unknown>(workflowUUID: string, functionID: number, topic: string, key: string, timeout: number): Promise<T | null> {
    throw new Error("Method not implemented.");
  }
}