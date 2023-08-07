/* eslint-disable @typescript-eslint/no-explicit-any */

import { deserializeError, serializeError } from "serialize-error";
import { operonNull, OperonNull, function_outputs, notifications } from "./operon";
import { DatabaseError, Pool, PoolClient, Notification } from 'pg';
import { OperonWorkflowConflictUUIDError } from "./error";

export interface SystemDatabase {
  init() : Promise<void>
  destroy() : Promise<void>
  listenerMap: Record<string, () => void>;

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

  notificationsClient: PoolClient | null = null;
  readonly listenerMap: Record<string, () => void> = {};

  constructor(readonly pool: Pool) {}

  async init() {
    await this.listenForNotifications();
  }

  async destroy() {
    if (this.notificationsClient) {
      this.notificationsClient.removeAllListeners();
      this.notificationsClient.release();
    }
  }

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

  /**
   *  Guard the operation, throwing an error if a conflicting execution is detected.
   */
  async guardOperation(client: PoolClient, workflowUUID: string, functionID: number) {
    try {
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
  }

  async send<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic: string, key: string, message: T): Promise<boolean> {
    const client: PoolClient = await this.pool.connect();

    await client.query("BEGIN");
    let { rows } = await client.query<function_outputs>("SELECT output, error FROM operon.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2",
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
    await this.guardOperation(client, workflowUUID, functionID);
    ({ rows } = await client.query(`INSERT INTO operon.notifications (topic, key, message) VALUES ($1, $2, $3)
      ON CONFLICT (topic, key) DO NOTHING RETURNING 'Success';`, [topic, key, JSON.stringify(message)]));
    const success: boolean = (rows.length !== 0); // Return true if successful, false if the key already exists.
    await client.query("UPDATE operon.operation_outputs SET output=$1 WHERE workflow_uuid=$2 AND function_id=$3;",
      [JSON.stringify(success), workflowUUID, functionID]);
    await client.query("COMMIT");
    client.release();
    return success;
  }

  async recv<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic: string, key: string, timeoutSeconds: number): Promise<T | null> {
    // First, check for previous executions.
    const checkRows = (await this.pool.query<function_outputs>("SELECT output, error FROM operon.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2",
      [workflowUUID, functionID])).rows;
    if (checkRows.length > 0) {
      if (JSON.parse(checkRows[0].error) !== null) {
        throw deserializeError(JSON.parse(checkRows[0].error));
      } else  {
        return JSON.parse(checkRows[0].output) as T;
      }
    }

    // Then, register the key with the global notifications listener.
    let resolveNotification: () => void;
    const messagePromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });
    this.listenerMap[`${topic}::${key}`] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.
    let timer: NodeJS.Timeout;
    const timeoutPromise = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        resolve();
      }, timeoutSeconds * 1000);
    });
    const received = Promise.race([messagePromise, timeoutPromise]);

    // Then, check if the key is already in the DB, returning it if it is.
    let client = await this.pool.connect();
    await client.query(`BEGIN`);
    await this.guardOperation(client, workflowUUID, functionID);
    const initRecvRows = (await client.query<notifications>("DELETE FROM operon.notifications WHERE topic=$1 AND key=$2 RETURNING message", [topic, key])).rows;
    if (initRecvRows.length > 0 ) {
      const message: T = JSON.parse(initRecvRows[0].message) as T;
      await client.query("UPDATE operon.operation_outputs SET output=$1 WHERE workflow_uuid=$2 AND function_id=$3;",
        [JSON.stringify(message), workflowUUID, functionID]);
      await client.query(`COMMIT`);
      client.release();
      delete this.listenerMap[`${topic}::${key}`];
      return message;
    } else {
      await client.query(`ROLLBACK`);
    }
    client.release();

    // Wait for the notification, then check if the key is in the DB, returning the message if it is and null if it isn't.
    await received;
    clearTimeout(timer!);
    client = await this.pool.connect();
    await client.query(`BEGIN`);
    await this.guardOperation(client, workflowUUID, functionID);
    const finalRecvRows = (await client.query<notifications>("DELETE FROM operon.notifications WHERE topic=$1 AND key=$2 RETURNING message", [topic, key])).rows;
    let message: T | null = null;
    if (finalRecvRows.length > 0 ) {
      message = JSON.parse(finalRecvRows[0].message) as T;
    }
    await client.query("UPDATE operon.operation_outputs SET output=$1 WHERE workflow_uuid=$2 AND function_id=$3;",
      [JSON.stringify(message), workflowUUID, functionID]);
    await client.query(`COMMIT`);
    client.release();
    return message;
  }
  
  /* BACKGROUND PROCESSES */
  /**
   * A background process that listens for notifications from Postgres then signals the appropriate
   * workflow listener by resolving its promise.
   */
  async listenForNotifications() {
    this.notificationsClient = await this.pool.connect();
    await this.notificationsClient.query('LISTEN operon_notifications_channel;');
    const handler = (msg: Notification ) => {
      if (msg.payload && msg.payload in this.listenerMap) {
        this.listenerMap[msg.payload]();
      }
    };
    this.notificationsClient.on('notification', handler);
  }
}