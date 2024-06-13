/* eslint-disable @typescript-eslint/no-explicit-any */

import { deserializeError, serializeError } from "serialize-error";
import { DBOSExecutor, dbosNull, DBOSNull } from "./dbos-executor";
import { DatabaseError, Pool, PoolClient, Notification, PoolConfig, Client } from "pg";
import { DuplicateWorkflowEventError, DBOSWorkflowConflictUUIDError, DBOSNonExistentWorkflowError } from "./error";
import { StatusString, WorkflowStatus } from "./workflow";
import { notifications, operation_outputs, workflow_status, workflow_events, workflow_inputs, scheduler_state } from "../schemas/system_db_schema";
import { sleepms, findPackageRoot, DBOSJSON } from "./utils";
import { HTTPRequest } from "./context";
import { GlobalLogger as Logger } from "./telemetry/logs";
import knex from "knex";
import path from "path";
import { createPlaceholders, prepareForSQL } from "./utils_sql";

interface QueriesAndValues {
  sql: string;
  params: Array<unknown>;
  ids: Array<string>
}

export interface SystemDatabase {
  init(): Promise<void>;
  destroy(): Promise<void>;

  checkWorkflowOutput<R>(workflowUUID: string): Promise<DBOSNull | R>;
  initWorkflowStatus<T extends any[]>(bufferedStatus: WorkflowStatusInternal, args: T): Promise<T>;
  bufferWorkflowOutput(workflowUUID: string, status: WorkflowStatusInternal): void;
  flushWorkflowSystemBuffers(): Promise<void>;
  recordWorkflowError(workflowUUID: string, status: WorkflowStatusInternal): Promise<void>;

  getPendingWorkflows(executorID: string): Promise<Array<string>>;
  bufferWorkflowInputs<T extends any[]>(workflowUUID: string, args: T): void;
  getWorkflowInputs<T extends any[]>(workflowUUID: string): Promise<T | null>;

  checkOperationOutput<R>(workflowUUID: string, functionID: number): Promise<DBOSNull | R>;
  recordOperationOutput<R>(workflowUUID: string, functionID: number, output: R): Promise<void>;
  recordOperationError(workflowUUID: string, functionID: number, error: Error): Promise<void>;

  getWorkflowStatus(workflowUUID: string, callerUUID?: string, functionID?: number): Promise<WorkflowStatus | null>;
  getWorkflowResult<R>(workflowUUID: string): Promise<R>;

  sleepms(workflowUUID: string, functionID: number, duration: number): Promise<void>;

  send<T>(workflowUUID: string, functionID: number, destinationUUID: string, message: T, topic?: string): Promise<void>;
  recv<T>(workflowUUID: string, functionID: number, topic?: string, timeoutSeconds?: number): Promise<T | null>;

  setEvent<T>(workflowUUID: string, functionID: number, key: string, value: T): Promise<void>;
  getEvent<T>(workflowUUID: string, key: string, timeoutSeconds: number, callerUUID?: string, functionID?: number): Promise<T | null>;

  // Scheduler queries
  //  These two maintain exactly once - make sure we kick off the workflow at least once, and wf unique ID does the rest
  getLastScheduledTime(wfn: string): Promise<number | null>; // Last workflow we are sure we invoked
  setLastScheduledTime(wfn: string, invtime: number): Promise<number | null>; // We are now sure we invoked another
}

// For internal use, not serialized status.
export interface WorkflowStatusInternal {
  workflowUUID: string;
  status: string;
  name: string;
  className: string;
  configName: string;
  authenticatedUser: string;
  output: unknown;
  error: string; // Serialized error
  assumedRole: string;
  authenticatedRoles: string[];
  request: HTTPRequest;
  executorID: string;
  applicationVersion: string;
  applicationID: string;
  createdAt: number;
}

export interface ExistenceCheck {
  exists: boolean;
}

export async function migrateSystemDatabase(systemPoolConfig: PoolConfig) {
  const migrationsDirectory = path.join(findPackageRoot(__dirname), 'migrations');
  const knexConfig = {
    client: 'pg',
    connection: systemPoolConfig,
    migrations: {
      directory: migrationsDirectory,
      tableName: 'knex_migrations'
    }
  };
  const knexDB = knex(knexConfig)
  await knexDB.migrate.latest()
  await knexDB.destroy()
}

export class PostgresSystemDatabase implements SystemDatabase {
  readonly pool: Pool;
  readonly systemPoolConfig: PoolConfig;

  notificationsClient: PoolClient | null = null;
  readonly notificationsMap: Record<string, () => void> = {};
  readonly workflowEventsMap: Record<string, () => void> = {};

  readonly workflowStatusBuffer: Map<string, WorkflowStatusInternal> = new Map();
  readonly workflowInputsBuffer: Map<string, any[]> = new Map();
  readonly flushBatchSize = 100;
  static readonly connectionTimeoutMillis = 10000; // 10 second timeout

  constructor(readonly pgPoolConfig: PoolConfig, readonly systemDatabaseName: string, readonly logger: Logger) {
    this.systemPoolConfig = { ...pgPoolConfig };
    this.systemPoolConfig.database = systemDatabaseName;
    this.systemPoolConfig.connectionTimeoutMillis = PostgresSystemDatabase.connectionTimeoutMillis;
    this.pool = new Pool(this.systemPoolConfig);
  }

  async init() {
    const pgSystemClient = new Client(this.pgPoolConfig);
    await pgSystemClient.connect();
    // Create the system database and load tables.
    const dbExists = await pgSystemClient.query<ExistenceCheck>(`SELECT EXISTS (SELECT FROM pg_database WHERE datname = '${this.systemDatabaseName}')`);
    if (!dbExists.rows[0].exists) {
      // Create the DBOS system database.
      await pgSystemClient.query(`CREATE DATABASE "${this.systemDatabaseName}"`);
    }
    await migrateSystemDatabase(this.systemPoolConfig);
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

  async checkWorkflowOutput<R>(workflowUUID: string): Promise<DBOSNull | R> {
    const { rows } = await this.pool.query<workflow_status>(`
      SELECT status, output, error
      FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
      WHERE workflow_uuid=$1`,
      [workflowUUID]);
    if (rows.length === 0 || rows[0].status === StatusString.PENDING) {
      return dbosNull;
    } else if (rows[0].status === StatusString.ERROR) {
      throw deserializeError(DBOSJSON.parse(rows[0].error));
    } else {
      return DBOSJSON.parse(rows[0].output) as R;
    }
  }

  async initWorkflowStatus<T extends any[]>(initStatus: WorkflowStatusInternal, args: T): Promise<T> {
    const workflow_status = prepareForSQL({
      "workflow_uuid": initStatus.workflowUUID,
      "status": initStatus.status,
      "name": initStatus.name,
      "class_name": initStatus.className,
      "config_name": initStatus.configName,
      "authenticated_user": initStatus.authenticatedUser,
      "assumed_role": initStatus.assumedRole,
      "authenticated_roles": DBOSJSON.stringify(initStatus.authenticatedRoles),
      "request": DBOSJSON.stringify(initStatus.request),
      "output": null,
      "executor_id": initStatus.executorID,
      "application_version": initStatus.applicationVersion,
      "application_id": initStatus.applicationID,
      "created_at": initStatus.createdAt,
    });

    await this.pool.query<workflow_status>(`
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status
        (${workflow_status.columns}) VALUES (${workflow_status.placeholders})
      ON CONFLICT (workflow_uuid)
        DO NOTHING`,
      workflow_status.values
    );

    const workflow_inputs = prepareForSQL({
      "workflow_uuid": initStatus.workflowUUID,
      "inputs": DBOSJSON.stringify(args),
    });

    const { rows } = await this.pool.query<workflow_inputs>(`
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_inputs
        (${workflow_inputs.columns}) VALUES (${workflow_inputs.placeholders})
      ON CONFLICT (workflow_uuid)
        DO UPDATE SET workflow_uuid = excluded.workflow_uuid
      RETURNING inputs`,
      workflow_inputs.values
    );
    return DBOSJSON.parse(rows[0].inputs) as T;
  }

  bufferWorkflowOutput(workflowUUID: string, status: WorkflowStatusInternal) {
    this.workflowStatusBuffer.set(workflowUUID, status);
  }

  /**
   * Flush the workflow output buffer and the input buffer to the database.
   */
  async flushWorkflowSystemBuffers(): Promise<void> {
    // Always flush the status buffer first because of foreign key constraints
    await this.flushWorkflowStatusBuffer();
    await this.flushWorkflowInputsBuffer();
  }

  private async flushWorkflowStatusBuffer(): Promise<void> {
    const localBuffer = new Map(this.workflowStatusBuffer);
    this.workflowStatusBuffer.clear();

    const totalSize = localBuffer.size;
    const columns = [
      "workflow_uuid",
      "status",
      "name",
      "authenticated_user",
      "assumed_role",
      "authenticated_roles",
      "request",
      "output",
      "executor_id",
      "application_version",
      "application_id",
      "created_at",
      "updated_at"
    ];

    const queriesAndValues : Array<QueriesAndValues> = [];

    try {
      let finishedCnt = 0;

      while (finishedCnt < totalSize) {
        let sqlStmt = `
          INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status
            (${columns.join(',')}) VALUES `;
        const values: unknown[] = [];
        const batchUUIDs: string[] = [];

        for (const [workflowUUID, status] of localBuffer) {
          const placeholders = createPlaceholders(columns.length, columns.length * finishedCnt)
          sqlStmt += `${finishedCnt ? ',' : ''} (${placeholders.join(',')}) `
          values.push(
            workflowUUID,
            status.status,
            status.name,
            status.authenticatedUser,
            status.assumedRole,
            DBOSJSON.stringify(status.authenticatedRoles),
            DBOSJSON.stringify(status.request),
            DBOSJSON.stringify(status.output),
            status.executorID,
            status.applicationVersion,
            status.applicationID,
            status.createdAt,
            Date.now()
          );
          batchUUIDs.push(workflowUUID);
          finishedCnt++;

          if (batchUUIDs.length >= this.flushBatchSize) {
            // Cap at the batch size.
            break;
          }
        }
        sqlStmt += " ON CONFLICT (workflow_uuid) DO UPDATE SET status=EXCLUDED.status, output=EXCLUDED.output, updated_at=EXCLUDED.updated_at;";
        queriesAndValues.push({
          sql: sqlStmt,
          params: values,
          ids: batchUUIDs
        })
      }
      const MAX_CONCURRENT_CONNECTIONS = 5;
      const slices: Array<Array<QueriesAndValues>> = [];
      for (let i = 0; i < queriesAndValues.length; i += MAX_CONCURRENT_CONNECTIONS) {
        const slice = queriesAndValues.slice(i, i + MAX_CONCURRENT_CONNECTIONS);
        slices.push(slice);
      }
      for (const slice of slices) {
        const promises = new Array<Promise<unknown>>();
        slice.forEach( (qv) => {
          const promise = this.pool.query(qv.sql, qv.params).then(() => {
            qv.ids.forEach((value) => {
              localBuffer.delete(value);
            });
          })
          promises.push(promise);
        });
        await Promise.all(promises);
      }
    } catch (error) {
      (error as Error).message = `Error flushing workflow status buffer: ${(error as Error).message}`;
      this.logger.error(error);
    } finally {
      // If there are still items in flushing the buffer, return items to the global buffer for retrying later.
      for (const [workflowUUID, output] of localBuffer) {
        if (!this.workflowStatusBuffer.has(workflowUUID)) {
          this.workflowStatusBuffer.set(workflowUUID, output);
        }
      }
    }
    return;
  }

  async recordWorkflowError(workflowUUID: string, status: WorkflowStatusInternal): Promise<void> {
    const { columns, placeholders, values } = prepareForSQL({
      "workflow_uuid": workflowUUID,
      "status": StatusString.ERROR,
      "name": status.name,
      "class_name": status.className,
      "config_name": status.configName,
      "authenticated_user": status.authenticatedUser,
      "assumed_role": status.assumedRole,
      "authenticated_roles": DBOSJSON.stringify(status.authenticatedRoles),
      "request": DBOSJSON.stringify(status.request),
      "error": status.error,
      "executor_id": status.executorID,
      "application_id": status.applicationID,
      "application_version": status.applicationVersion,
      "created_at": status.createdAt,
      "updated_at": Date.now(),
    });
    await this.pool.query<workflow_status>(`
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status
        (${columns}) VALUES (${placeholders})
      ON CONFLICT (workflow_uuid)
        DO UPDATE SET status=EXCLUDED.status, error=EXCLUDED.error, updated_at=EXCLUDED.updated_at;`,
      values
    );
  }

  async getPendingWorkflows(executorID: string): Promise<Array<string>> {
    const { rows } = await this.pool.query<workflow_status>(`
      SELECT workflow_uuid FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
      WHERE status=$1 AND executor_id=$2`,
      [StatusString.PENDING, executorID]
    )
    return rows.map(i => i.workflow_uuid);
  }

  bufferWorkflowInputs<T extends any[]>(workflowUUID: string, args: T): void {
    this.workflowInputsBuffer.set(workflowUUID, args);
  }

  async flushWorkflowInputsBuffer(): Promise<void> {
    const localBuffer = new Map(this.workflowInputsBuffer);
    this.workflowInputsBuffer.clear();
    const totalSize = localBuffer.size;
    try {
      let finishedCnt = 0;
      while (finishedCnt < totalSize) {
        let sqlStmt = `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_inputs (workflow_uuid, inputs) VALUES `;
        let paramCnt = 1;
        const values: any[] = [];
        const batchUUIDs: string[] = [];
        for (const [workflowUUID, args] of localBuffer) {
          finishedCnt++;
          if (this.workflowStatusBuffer.has(workflowUUID)) {
            // Need the workflow status buffer to be flushed first. Continue and retry later.
            continue;
          }

          if (paramCnt > 1) {
            sqlStmt += ", ";
          }
          sqlStmt += `($${paramCnt++}, $${paramCnt++})`;
          values.push(workflowUUID, DBOSJSON.stringify(args));
          batchUUIDs.push(workflowUUID);

          if (batchUUIDs.length >= this.flushBatchSize) {
            // Cap at the batch size.
            break;
          }
        }

        if (batchUUIDs.length > 0) {
          sqlStmt += " ON CONFLICT (workflow_uuid) DO NOTHING;";
          await this.pool.query(sqlStmt, values);
          // Clean up after each batch succeeds
          batchUUIDs.forEach((value) => { localBuffer.delete(value); });
        }
      }
    } catch (error) {
      (error as Error).message = `Error flushing workflow inputs buffer: ${(error as Error).message}`;
      this.logger.error(error);
    } finally {
      // If there are still items in flushing the buffer, return items to the global buffer for retrying later.
      for (const [workflowUUID, args] of localBuffer) {
        if (!this.workflowInputsBuffer.has(workflowUUID)) {
          this.workflowInputsBuffer.set(workflowUUID, args);
        }
      }
    }
    return;
  }

  async getWorkflowInputs<T extends any[]>(workflowUUID: string): Promise<T | null> {
    const { rows } = await this.pool.query<workflow_inputs>(`
      SELECT inputs
      FROM ${DBOSExecutor.systemDBSchemaName}.workflow_inputs
      WHERE workflow_uuid=$1`,
      [workflowUUID]
    )
    if (rows.length === 0) {
      return null
    }
    return DBOSJSON.parse(rows[0].inputs) as T;
  }

  async checkOperationOutput<R>(workflowUUID: string, functionID: number): Promise<DBOSNull | R> {
    const { rows } = await this.pool.query<operation_outputs>(`
      SELECT output, error
      FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`,
      [workflowUUID, functionID]);
    if (rows.length === 0) {
      return dbosNull;
    } else if (DBOSJSON.parse(rows[0].error) !== null) {
      throw deserializeError(DBOSJSON.parse(rows[0].error));
    } else {
      return DBOSJSON.parse(rows[0].output) as R;
    }
  }

  async recordOperationOutput<R>(workflowUUID: string, functionID: number, output: R): Promise<void> {
    const { columns, placeholders, values } = prepareForSQL({
      "workflow_uuid": workflowUUID,
      "function_id": functionID,
      "output": DBOSJSON.stringify(output)
    });

    try {
      await this.pool.query<operation_outputs>(`
        INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs
          (${columns}) VALUES (${placeholders});`,
        values);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === "40001" || err.code === "23505") {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(workflowUUID);
      } else {
        throw err;
      }
    }
  }

  async recordOperationError(workflowUUID: string, functionID: number, error: Error): Promise<void> {
    const { columns, placeholders, values } = prepareForSQL({
      "workflow_uuid": workflowUUID,
      "function_id": functionID,
      "error": DBOSJSON.stringify(serializeError(error))
    });
    try {
      await this.pool.query<operation_outputs>(`
        INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs
          (${columns}) VALUES (${placeholders});`,
        values);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === "40001" || err.code === "23505") {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(workflowUUID);
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
      const { columns, placeholders, values } = prepareForSQL({
        "workflow_uuid": workflowUUID,
        "function_id": functionID,
        "output": DBOSJSON.stringify(output)
      });
      await client.query<operation_outputs>(`
        INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs
          (${columns}) VALUES (${placeholders});`,
        values);
    } catch (error) {
      await client.query("ROLLBACK");
      client.release();
      const err: DatabaseError = error as DatabaseError;
      if (err.code === "40001" || err.code === "23505") {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(workflowUUID);
      } else {
        throw err;
      }
    }
  }

  async sleepms(workflowUUID: string, functionID: number, durationMS: number): Promise<void> {
    const { rows } = await this.pool.query<operation_outputs>(`
      SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`,
      [workflowUUID, functionID]);
    if (rows.length > 0) {
      const endTimeMs = DBOSJSON.parse(rows[0].output) as number;
      await sleepms(Math.max(endTimeMs - Date.now(), 0))
      return;
    } else {
      const endTimeMs = Date.now() + durationMS;
      const { columns, placeholders, values } = prepareForSQL({
        "workflow_uuid": workflowUUID,
        "function_id": functionID,
        "output": DBOSJSON.stringify(endTimeMs)
      });
      await this.pool.query<operation_outputs>(`
        INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs
          (${columns}) VALUES (${placeholders}) ON CONFLICT DO NOTHING;`,
        values);
      await sleepms(Math.max(endTimeMs - Date.now(), 0))
      return;
    }
  }

  readonly nullTopic = "__null__topic__";

  async send<T>(workflowUUID: string, functionID: number, destinationUUID: string, message: T, topic?: string): Promise<void> {
    topic = topic ?? this.nullTopic;
    const client: PoolClient = await this.pool.connect();

    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    const { rows } = await client.query<operation_outputs>(`
      SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`,
      [workflowUUID, functionID]);
    if (rows.length > 0) {
      await client.query("ROLLBACK");
      client.release();
      return;
    }

    try {
      const { columns, placeholders, values } = prepareForSQL({
        "destination_uuid": destinationUUID,
        "topic": topic,
        "message": DBOSJSON.stringify(message)
      });

      await client.query(`
        INSERT INTO ${DBOSExecutor.systemDBSchemaName}.notifications
        (${columns}) VALUES (${placeholders});`,
        values
      );
    } catch (error) {
      await client.query("ROLLBACK");
      client.release();
      const err: DatabaseError = error as DatabaseError;
      if (err.code === "23503") {
        // Foreign key constraint violation
        throw new DBOSNonExistentWorkflowError(`Sent to non-existent destination workflow UUID: ${destinationUUID}`);
      } else {
        throw err;
      }
    }

    await this.recordNotificationOutput(client, workflowUUID, functionID, undefined);
    await client.query("COMMIT");
    client.release();
  }

  async recv<T>(workflowUUID: string, functionID: number, topic?: string, timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec): Promise<T | null> {
    topic = topic ?? this.nullTopic;
    // First, check for previous executions.
    const checkRows = (await this.pool.query<operation_outputs>(`
      SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`,
      [workflowUUID, functionID])).rows;
    if (checkRows.length > 0) {
      return DBOSJSON.parse(checkRows[0].output) as T;
    }

    // Then, register the key with the global notifications listener.
    let resolveNotification: () => void;
    const messagePromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });
    const payload = `${workflowUUID}::${topic}`;
    this.notificationsMap[payload] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.
    let timer: NodeJS.Timeout;
    const timeoutPromise = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        resolve();
      }, timeoutSeconds * 1000);
    });
    const received = Promise.race([messagePromise, timeoutPromise]);

    // Check if the key is already in the DB, then wait for the notification if it isn't.
    const initRecvRows = (await this.pool.query<notifications>(`
      SELECT topic FROM ${DBOSExecutor.systemDBSchemaName}.notifications
      WHERE destination_uuid=$1 AND topic=$2;`,
      [workflowUUID, topic])).rows;
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
        FROM ${DBOSExecutor.systemDBSchemaName}.notifications
        WHERE destination_uuid = $1
          AND topic = $2
        ORDER BY created_at_epoch_ms ASC
        LIMIT 1
      )

      DELETE FROM ${DBOSExecutor.systemDBSchemaName}.notifications
      USING oldest_entry
      WHERE notifications.destination_uuid = oldest_entry.destination_uuid
        AND notifications.topic = oldest_entry.topic
        AND notifications.created_at_epoch_ms = oldest_entry.created_at_epoch_ms
      RETURNING notifications.*;`,
      [workflowUUID, topic])).rows;
    let message: T | null = null;
    if (finalRecvRows.length > 0) {
      message = DBOSJSON.parse(finalRecvRows[0].message) as T;
    }
    await this.recordNotificationOutput(client, workflowUUID, functionID, message);
    await client.query(`COMMIT`);
    client.release();
    return message;
  }

  async setEvent<T>(workflowUUID: string, functionID: number, key: string, message: T): Promise<void> {
    const client: PoolClient = await this.pool.connect();

    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    let { rows } = await client.query<operation_outputs>(`
      SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`,
      [workflowUUID, functionID]);
    if (rows.length > 0) {
      await client.query("ROLLBACK");
      client.release();
      return;
    }
    ({ rows } = await client.query(`
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_events
        (workflow_uuid, key, value) VALUES ($1, $2, $3)
      ON CONFLICT (workflow_uuid, key)
      DO NOTHING RETURNING workflow_uuid;`,
      [workflowUUID, key, DBOSJSON.stringify(message)]
    ));
    if (rows.length === 0) {
      await client.query("ROLLBACK");
      client.release();
      throw new DuplicateWorkflowEventError(workflowUUID, key);
    }
    await this.recordNotificationOutput(client, workflowUUID, functionID, undefined);
    await client.query("COMMIT");
    client.release();
  }

  async getEvent<T>(workflowUUID: string, key: string, timeoutSeconds: number, callerUUID?: string, functionID?: number): Promise<T | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    if (callerUUID !== undefined && functionID !== undefined) {
      const { rows } = await this.pool.query<operation_outputs>(`
        SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
        WHERE workflow_uuid=$1 AND function_id=$2`,
        [callerUUID, functionID]);
      if (rows.length > 0) {
        return DBOSJSON.parse(rows[0].output) as T;
      }
    }

    // Register the key with the global notifications listener.
    let resolveNotification: () => void;
    const valuePromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });
    this.workflowEventsMap[`${workflowUUID}::${key}`] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.
    let timer: NodeJS.Timeout;
    const timeoutPromise = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        resolve();
      }, timeoutSeconds * 1000);
    });
    const received = Promise.race([valuePromise, timeoutPromise]);

    // Check if the key is already in the DB, then wait for the notification if it isn't.
    const initRecvRows = (await this.pool.query<workflow_events>(`
      SELECT key, value FROM ${DBOSExecutor.systemDBSchemaName}.workflow_events
      WHERE workflow_uuid=$1 AND key=$2;`,
      [workflowUUID, key])).rows;
    if (initRecvRows.length === 0) {
      await received;
    }
    clearTimeout(timer!);

    // Return the value if it's in the DB, otherwise return null.
    let value: T | null = null;
    if (initRecvRows.length > 0) {
      value = DBOSJSON.parse(initRecvRows[0].value) as T;
    } else {
      // Read it again from the database.
      const finalRecvRows = (await this.pool.query<workflow_events>(`
        SELECT value FROM ${DBOSExecutor.systemDBSchemaName}.workflow_events
        WHERE workflow_uuid=$1 AND key=$2;`,
        [workflowUUID, key])).rows;
      if (finalRecvRows.length > 0) {
        value = DBOSJSON.parse(finalRecvRows[0].value) as T;
      }
    }

    // Record the output if it is inside a workflow.
    if (callerUUID !== undefined && functionID !== undefined) {
      await this.recordOperationOutput(callerUUID, functionID, value);
    }
    return value;
  }

  async getWorkflowStatus(workflowUUID: string, callerUUID?: string, functionID?: number): Promise<WorkflowStatus | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    if (callerUUID !== undefined && functionID !== undefined) {
      const { rows } = await this.pool.query<operation_outputs>(`
        SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
        WHERE workflow_uuid=$1 AND function_id=$2`,
        [callerUUID, functionID]);
      if (rows.length > 0) {
        return DBOSJSON.parse(rows[0].output) as WorkflowStatus;
      }
    }

    const { rows } = await this.pool.query<workflow_status>(`
      SELECT status, name, class_name, config_name, authenticated_user, assumed_role, authenticated_roles, request
      FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
      WHERE workflow_uuid=$1`,
      [workflowUUID]);
    let value = null;
    if (rows.length > 0) {
      value = {
        status: rows[0].status,
        workflowName: rows[0].name,
        workflowClassName: rows[0].class_name || "",
        workflowConfigName: rows[0].config_name || "",
        authenticatedUser: rows[0].authenticated_user,
        assumedRole: rows[0].assumed_role,
        authenticatedRoles: DBOSJSON.parse(rows[0].authenticated_roles) as string[],
        request: DBOSJSON.parse(rows[0].request) as HTTPRequest,
      };
    }

    // Record the output if it is inside a workflow.
    if (callerUUID !== undefined && functionID !== undefined) {
      await this.recordOperationOutput(callerUUID, functionID, value);
    }
    return value;
  }

  async getWorkflowResult<R>(workflowUUID: string): Promise<R> {
    const pollingIntervalMs: number = 1000;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const { rows } = await this.pool.query<workflow_status>(`
        SELECT status, output, error
        FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
        WHERE workflow_uuid=$1`,
        [workflowUUID]);
      if (rows.length > 0) {
        const status = rows[0].status;
        if (status === StatusString.SUCCESS) {
          return DBOSJSON.parse(rows[0].output) as R;
        } else if (status === StatusString.ERROR) {
          throw deserializeError(DBOSJSON.parse(rows[0].error));
        }
      }
      await sleepms(pollingIntervalMs);
    }
  }

  /* BACKGROUND PROCESSES */
  /**
   * A background process that listens for notifications from Postgres then signals the appropriate
   * workflow listener by resolving its promise.
   */
  async listenForNotifications() {
    this.notificationsClient = await this.pool.connect();
    await this.notificationsClient.query("LISTEN dbos_notifications_channel;");
    await this.notificationsClient.query("LISTEN dbos_workflow_events_channel;");
    const handler = (msg: Notification) => {
      if (msg.channel === 'dbos_notifications_channel') {
        if (msg.payload && msg.payload in this.notificationsMap) {
          this.notificationsMap[msg.payload]();
        }
      } else {
        if (msg.payload && msg.payload in this.workflowEventsMap) {
          this.workflowEventsMap[msg.payload]();
        }
      }
    };
    this.notificationsClient.on("notification", handler);
  }

  /* SCHEDULER */
  async getLastScheduledTime(wfn: string): Promise<number | null> {
    const res = await this.pool.query<scheduler_state>(`
      SELECT last_run_time
      FROM ${DBOSExecutor.systemDBSchemaName}.scheduler_state
      WHERE workflow_fn_name = $1;
    `, [wfn]);

    let v = res.rows[0]?.last_run_time ?? null;
    if (v !== null) v = parseInt(`${v}`);
    return v;
  }

  async setLastScheduledTime(wfn: string, invtime: number): Promise<number | null> {
    const res = await this.pool.query<scheduler_state>(`
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.scheduler_state
      (workflow_fn_name, last_run_time) VALUES ($1, $2)
      ON CONFLICT (workflow_fn_name)
      DO UPDATE SET last_run_time = GREATEST(EXCLUDED.last_run_time, scheduler_state.last_run_time)
      RETURNING last_run_time;
    `, [wfn, invtime]);

    return parseInt(`${res.rows[0].last_run_time}`);
  }
}
