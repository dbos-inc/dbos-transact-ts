/* eslint-disable @typescript-eslint/no-explicit-any */

import { deserializeError, serializeError } from "serialize-error";
import { operonNull, OperonNull, function_outputs } from "./operon";
import { DatabaseError, Pool, PoolClient } from 'pg';
import { OperonWorkflowConflictUUIDError } from "./error";

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
    try {
      await this.pool.query("INSERT INTO operon.operation_outputs (workflow_uuid, function_id, output) VALUES ($1, $2, $3);",
        [workflowUUID, functionID, serialOutput]);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '40001' || err.code === '23505') { // Serialization and primary key conflict (Postgres).
        throw new OperonWorkflowConflictUUIDError();
      } else {
        throw err;
      }
    }
  }

  async recordCommunicatorError(workflowUUID: string, functionID: number, error: Error): Promise<void> {
    const serialErr = JSON.stringify(serializeError(error));
    try {
      await this.pool.query("INSERT INTO operon.operation_outputs (workflow_uuid, function_id, error) VALUES ($1, $2, $3);",
        [workflowUUID, functionID, serialErr]);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '40001' || err.code === '23505') { // Serialization and primary key conflict (Postgres).
        throw new OperonWorkflowConflictUUIDError();
      } else {
        throw err;
      }
    }
  }

  async send<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic: string, key: string, message: T): Promise<boolean> {
    const client: PoolClient = await this.pool.connect();

    await client.query("BEGIN");
    let { rows } = await this.pool.query<function_outputs>("SELECT output, error FROM operon.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2",
      [workflowUUID, functionID]);
    if (rows.length > 0) {
      await client.query("ROLLBACK");
      client.release();
      if (JSON.parse(rows[0].error) !== null) {
        throw deserializeError(JSON.parse(rows[0].error));
      } else  {
        return JSON.parse(rows[0].output) as boolean;
      }
    }
    try { // Guard the operation, throwing an error if a conflicting execution is detected.
      await client.query("INSERT INTO operon.operation_outputs (workflow_uuid, function_id) VALUES ($1, $2);",
        [workflowUUID, functionID]);
    } catch (error) {
      await client.query("ROLLBACK");
      client.release();
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '40001' || err.code === '23505') { // Serialization and primary key conflict (Postgres).
        throw new OperonWorkflowConflictUUIDError();
      } else {
        throw err;
      }
    }
    ({ rows } = await client.query(`INSERT INTO operon.notifications (topic, key, message) VALUES ($1, $2, $3)
      ON CONFLICT (topic, key) DO NOTHING RETURNING 'Success';`, [topic, key, JSON.stringify(message)]));
    const success: boolean = (rows.length !== 0); // Return true if successful, false if the key already exists.
    await client.query("UPDATE operon.operation_outputs SET output=$1 WHERE workflow_uuid=$2 AND function_id=$3;",
      [JSON.stringify(success), workflowUUID, functionID]);
    await client.query("COMMIT");
    client.release();
    return success;
  }

  recv<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic: string, key: string, timeout: number): Promise<T | null> {
    throw new Error("Method not implemented.");
  }
}