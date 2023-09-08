/* eslint-disable @typescript-eslint/no-explicit-any */

import { deserializeError, serializeError } from "serialize-error";
import { operonNull, OperonNull } from "./operon";
import { DatabaseError, Pool, PoolClient, Notification, PoolConfig, Client } from "pg";
import { OperonDuplicateWorkflowValuesError, OperonWorkflowConflictUUIDError } from "./error";
import { StatusString, WorkflowStatus } from "./workflow";
import { systemDBSchema, notifications, operation_outputs, workflow_status, workflow_values } from "../schemas/system_db_schema";
import { sleep } from "./utils";

export interface SystemDatabase {
  init(): Promise<void>;
  destroy(): Promise<void>;

  checkWorkflowOutput<R>(workflowUUID: string): Promise<OperonNull | R>;
  bufferWorkflowStatus(workflowUUID: string): void;
  bufferWorkflowOutput<R>(workflowUUID: string, output: R): void;
  flushWorkflowStatusBuffer(): Promise<Array<string>>;
  recordWorkflowError(workflowUUID: string, error: Error): Promise<void>;

  checkCommunicatorOutput<R>(workflowUUID: string, functionID: number): Promise<OperonNull | R>;
  recordCommunicatorOutput<R>(workflowUUID: string, functionID: number, output: R): Promise<void>;
  recordCommunicatorError(workflowUUID: string, functionID: number, error: Error): Promise<void>;

  getWorkflowStatus(workflowUUID: string): Promise<WorkflowStatus>;
  getWorkflowResult<R>(workflowUUID: string): Promise<R>;

  send<T extends NonNullable<any>>(workflowUUID: string, functionID: number, destinationUUID: string, topic: string | null, message: T): Promise<void>;
  recv<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic: string | null, timeoutSeconds: number): Promise<T | null>;

  set<T extends NonNullable<any>>(workflowUUID: string, functionID: number, key: string, value: T) : Promise<void>;
  get<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number) : Promise<T | null>;
}

export class PostgresSystemDatabase implements SystemDatabase {
  readonly pool: Pool;

  notificationsClient: PoolClient | null = null;
  readonly notificationsMap: Record<string, () => void> = {};
  readonly workflowValuesMap: Record<string, () => void> = {};

  readonly workflowStatusBuffer: Map<string, any> = new Map();

  constructor(readonly pgPoolConfig: PoolConfig, readonly systemDatabaseName: string) {
    const poolConfig = { ...pgPoolConfig };
    poolConfig.database = systemDatabaseName;
    this.pool = new Pool(poolConfig);
  }

  async init() {
    const pgSystemClient = new Client(this.pgPoolConfig);
    await pgSystemClient.connect();
    // Create the system database and load tables.
    const dbExists = await pgSystemClient.query(`SELECT FROM pg_database WHERE datname = '${this.systemDatabaseName}'`);
    if (dbExists.rows.length === 0) {
      // Create the Operon system database.
      await pgSystemClient.query(`CREATE DATABASE "${this.systemDatabaseName}"`);
    }
    // Load the Operon system schemas.
    await this.pool.query(systemDBSchema);
    await this.listenForNotifications();
    await pgSystemClient.end();
  }

  async destroy() {
    if (this.notificationsClient) {
      this.notificationsClient.removeAllListeners();
      this.notificationsClient.release();
    }
    await this.pool.end();
  }

  async checkWorkflowOutput<R>(workflowUUID: string): Promise<OperonNull | R> {
    const { rows } = await this.pool.query<workflow_status>("SELECT status, output, error FROM operon.workflow_status WHERE workflow_uuid=$1", [workflowUUID]);
    // TODO: maybe add back pending state.
    if (rows.length === 0) {
      return operonNull;
    } else if (rows[0].status === StatusString.ERROR) {
      throw deserializeError(JSON.parse(rows[0].error));
    } else {
      return JSON.parse(rows[0].output) as R;
    }
  }

  bufferWorkflowStatus(workflowUUID: string) {
    this.workflowStatusBuffer.set(workflowUUID, operonNull);
  }

  bufferWorkflowOutput<R>(workflowUUID: string, output: R) {
    this.workflowStatusBuffer.set(workflowUUID, output);
  }

  /**
   * Flush the workflow output buffer to the database.
   */
  async flushWorkflowStatusBuffer() {
    const localBuffer = new Map(this.workflowStatusBuffer);
    this.workflowStatusBuffer.clear();
    const client: PoolClient = await this.pool.connect();
    await client.query("BEGIN");
    for (const [workflowUUID, output] of localBuffer) {
      if (output === operonNull) {
        await client.query(
          `INSERT INTO operon.workflow_status (workflow_uuid, status, output) VALUES($1, $2, $3) ON CONFLICT (workflow_uuid) DO NOTHING`,
          [workflowUUID, StatusString.PENDING, null]
        );
      } else {
        await client.query(
          `INSERT INTO operon.workflow_status (workflow_uuid, status, output) VALUES($1, $2, $3) ON CONFLICT (workflow_uuid)
        DO UPDATE SET status=EXCLUDED.status, output=EXCLUDED.output;`,
          [workflowUUID, StatusString.SUCCESS, JSON.stringify(output)]
        );
      }
    }
    await client.query("COMMIT");
    client.release();
    return Array.from(localBuffer.keys());
  }

  async recordWorkflowError(workflowUUID: string, error: Error): Promise<void> {
    const serialErr = JSON.stringify(serializeError(error));
    await this.pool.query(
      `INSERT INTO operon.workflow_status (workflow_uuid, status, error) VALUES($1, $2, $3) ON CONFLICT (workflow_uuid)
    DO UPDATE SET status=EXCLUDED.status, error=EXCLUDED.error;`,
      [workflowUUID, StatusString.ERROR, serialErr]
    );
  }

  async checkCommunicatorOutput<R>(workflowUUID: string, functionID: number): Promise<OperonNull | R> {
    const { rows } = await this.pool.query<operation_outputs>("SELECT output, error FROM operon.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2", [workflowUUID, functionID]);
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
      await this.pool.query("INSERT INTO operon.operation_outputs (workflow_uuid, function_id, output) VALUES ($1, $2, $3);", [workflowUUID, functionID, serialOutput]);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === "40001" || err.code === "23505") {
        // Serialization and primary key conflict (Postgres).
        throw new OperonWorkflowConflictUUIDError(workflowUUID);
      } else {
        throw err;
      }
    }
  }

  async recordCommunicatorError(workflowUUID: string, functionID: number, error: Error): Promise<void> {
    const serialErr = JSON.stringify(serializeError(error));
    try {
      await this.pool.query("INSERT INTO operon.operation_outputs (workflow_uuid, function_id, error) VALUES ($1, $2, $3);", [workflowUUID, functionID, serialErr]);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === "40001" || err.code === "23505") {
        // Serialization and primary key conflict (Postgres).
        throw new OperonWorkflowConflictUUIDError(workflowUUID);
      } else {
        throw err;
      }
    }
  }

  /**
   *  Guard the operation, throwing an error if a conflicting execution is detected.
   */
  async recordNotificationOutput<R>(client: PoolClient, workflowUUID: string, functionID: number, output: R) {
    try {
      await client.query("INSERT INTO operon.operation_outputs (workflow_uuid, function_id, output) VALUES ($1, $2, $3);", [workflowUUID, functionID, JSON.stringify(output)]);
    } catch (error) {
      await client.query("ROLLBACK");
      client.release();
      const err: DatabaseError = error as DatabaseError;
      if (err.code === "40001" || err.code === "23505") {
        // Serialization and primary key conflict (Postgres).
        throw new OperonWorkflowConflictUUIDError(workflowUUID);
      } else {
        throw err;
      }
    }
  }

  readonly nullTopic = "__null__topic__";

  async send<T extends NonNullable<any>>(workflowUUID: string, functionID: number, destinationUUID: string, topic: string | null, message: T): Promise<void> {
    topic = topic === null ? this.nullTopic : topic;
    const client: PoolClient = await this.pool.connect();

    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    const { rows } = await client.query<operation_outputs>("SELECT output FROM operon.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2", [workflowUUID, functionID]);
    if (rows.length > 0) {
      await client.query("ROLLBACK");
      client.release();
      return;
    }
    await client.query(
      `INSERT INTO operon.notifications (destination_uuid, topic, message) VALUES ($1, $2, $3);`,
      [destinationUUID, topic, JSON.stringify(message)]
    );
    await this.recordNotificationOutput(client, workflowUUID, functionID, undefined);
    await client.query("COMMIT");
    client.release();
  }

  async recv<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic: string | null, timeoutSeconds: number): Promise<T | null> {
    topic = topic === null ? this.nullTopic : topic;
    // First, check for previous executions.
    const checkRows = (await this.pool.query<operation_outputs>("SELECT output FROM operon.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2", [workflowUUID, functionID])).rows;
    if (checkRows.length > 0) {
      return JSON.parse(checkRows[0].output) as T;
    }

    // Then, register the key with the global notifications listener.
    let resolveNotification: () => void;
    const messagePromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });
    const payload = topic === null ? workflowUUID : `${workflowUUID}::${topic}`;
    this.notificationsMap[payload] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.
    let timer: NodeJS.Timeout;
    const timeoutPromise = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        resolve();
      }, timeoutSeconds * 1000);
    });
    const received = Promise.race([messagePromise, timeoutPromise]);

    // Check if the key is already in the DB, then wait for the notification if it isn't.
    const initRecvRows = (await this.pool.query<notifications>("SELECT topic FROM operon.notifications WHERE destination_uuid=$1 AND topic=$2;", [workflowUUID, topic])).rows;
    if (initRecvRows.length === 0) {
      await received;
    }
    clearTimeout(timer!);

    // Transactionally consume and return the message if it's in the DB, otherwise return null.
    const client = await this.pool.connect();
    await client.query(`BEGIN ISOLATION LEVEL READ COMMITTED`);
    const finalRecvRows = (await client.query<notifications>(
      `WITH oldest_entry AS (
        SELECT destination_uuid, topic, message, created_at_epoch_ms
        FROM operon.notifications
        WHERE destination_uuid = $1
          AND topic = $2
        ORDER BY created_at_epoch_ms ASC
        LIMIT 1
      )

      DELETE FROM operon.notifications
      USING oldest_entry
      WHERE operon.notifications.destination_uuid = oldest_entry.destination_uuid
        AND operon.notifications.topic = oldest_entry.topic
        AND operon.notifications.created_at_epoch_ms = oldest_entry.created_at_epoch_ms
      RETURNING operon.notifications.*;`,
      [workflowUUID, topic])).rows;
    let message: T | null = null;
    if (finalRecvRows.length > 0) {
      message = JSON.parse(finalRecvRows[0].message) as T;
    }
    await this.recordNotificationOutput(client, workflowUUID, functionID, message);
    await client.query(`COMMIT`);
    client.release();
    return message;
  }

  async set<T extends NonNullable<any>>(workflowUUID: string, functionID: number, key: string, message: T): Promise<void> {
    const client: PoolClient = await this.pool.connect();

    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    let { rows } = await client.query<operation_outputs>("SELECT output FROM operon.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2", [workflowUUID, functionID]);
    if (rows.length > 0) {
      await client.query("ROLLBACK");
      client.release();
      return;
    }
    ( { rows } = await client.query(
      `INSERT INTO operon.workflow_values (workflow_uuid, key, value) VALUES ($1, $2, $3) ON CONFLICT (workflow_uuid, key) DO NOTHING RETURNING workflow_uuid;`,
      [workflowUUID, key, JSON.stringify(message)]
    ));
    if (rows.length === 0) {
      await client.query("ROLLBACK");
      client.release();
      throw new OperonDuplicateWorkflowValuesError(workflowUUID, key);
    }
    await this.recordNotificationOutput(client, workflowUUID, functionID, undefined);
    await client.query("COMMIT");
    client.release();
  }

  async get<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number): Promise<T | null> {
    // Register the key with the global notifications listener.
    let resolveNotification: () => void;
    const valuePromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });
    this.workflowValuesMap[`${workflowUUID}::${key}`] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.
    let timer: NodeJS.Timeout;
    const timeoutPromise = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        resolve();
      }, timeoutSeconds * 1000);
    });
    const received = Promise.race([valuePromise, timeoutPromise]);

    // Check if the key is already in the DB, then wait for the notification if it isn't.
    const initRecvRows = (await this.pool.query<workflow_values>("SELECT key FROM operon.workflow_values WHERE workflow_uuid=$1 AND key=$2;", [workflowUUID, key])).rows;
    if (initRecvRows.length === 0) {
      await received;
    }
    clearTimeout(timer!);

    // Return the value if it's in the DB, otherwise return null.
    const finalRecvRows = (await this.pool.query<workflow_values>("SELECT value FROM operon.workflow_values WHERE workflow_uuid=$1 AND key=$2;", [workflowUUID, key])).rows;
    let value: T | null = null;
    if (finalRecvRows.length > 0) {
      value = JSON.parse(finalRecvRows[0].value) as T;
    }
    return value;
   }

  async getWorkflowStatus(workflowUUID: string): Promise<WorkflowStatus> {
    const { rows } = await this.pool.query<workflow_status>("SELECT status FROM operon.workflow_status WHERE workflow_uuid=$1", [workflowUUID]);
    if (rows.length === 0) {
      return { status: StatusString.UNKNOWN };
    }
    return { status: rows[0].status };
  }

  async getWorkflowResult<R>(workflowUUID: string): Promise<R> {
    const pollingIntervalMs: number = 1000;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const { rows } = await this.pool.query<workflow_status>("SELECT status, output, error FROM operon.workflow_status WHERE workflow_uuid=$1", [workflowUUID]);
      if (rows.length > 0) {
        const status = rows[0].status;
        if (status === StatusString.SUCCESS) {
          return JSON.parse(rows[0].output) as R;
        } else if (status === StatusString.ERROR) {
          throw deserializeError(JSON.parse(rows[0].error));
        }
      }
      await sleep(pollingIntervalMs);
    }
  }

  /* BACKGROUND PROCESSES */
  /**
   * A background process that listens for notifications from Postgres then signals the appropriate
   * workflow listener by resolving its promise.
   */
  async listenForNotifications() {
    this.notificationsClient = await this.pool.connect();
    await this.notificationsClient.query("LISTEN operon_notifications_channel;");
    await this.notificationsClient.query("LISTEN operon_workflow_values_channel;");
    const handler = (msg: Notification) => {
      if (msg.channel === 'operon_notifications_channel') {
        if (msg.payload && msg.payload in this.notificationsMap) {
          this.notificationsMap[msg.payload]();
        }
      } else {
        if (msg.payload && msg.payload in this.workflowValuesMap) {
          this.workflowValuesMap[msg.payload]();
        }
      }
    };
    this.notificationsClient.on("notification", handler);
  }
}
