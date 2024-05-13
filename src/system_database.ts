/* eslint-disable @typescript-eslint/no-explicit-any */

import { deserializeError, serializeError } from "serialize-error";
import { DBOSExecutor, dbosNull, DBOSNull } from "./dbos-executor";
import { DatabaseError, Pool, PoolClient, Notification, PoolConfig, Client } from "pg";
import { DuplicateWorkflowEventError, DBOSWorkflowConflictUUIDError, DBOSNonExistentWorkflowError } from "./error";
import { StatusString, WorkflowStatus } from "./workflow";
import { notifications, operation_outputs, workflow_status, workflow_events, workflow_inputs, scheduler_state } from "../schemas/system_db_schema";
import { sleepms, findPackageRoot, DBOSReplacer, DBOSReviver } from "./utils";
import { HTTPRequest } from "./context";
import { GlobalLogger as Logger } from "./telemetry/logs";
import knex from 'knex';
import path from "path";

export interface SystemDatabase {
  init(): Promise<void>;
  destroy(): Promise<void>;

  checkWorkflowOutput<R>(workflowUUID: string): Promise<DBOSNull | R>;
  initWorkflowStatus<T extends any[]>(bufferedStatus: WorkflowStatusInternal, args: T): Promise<T>;
  bufferWorkflowOutput(workflowUUID: string, status: WorkflowStatusInternal): void;
  flushWorkflowSystemBuffers(): Promise<void>;
  recordWorkflowError(workflowUUID: string, status: WorkflowStatusInternal): Promise<void>;

  getPendingWorkflows(executorID: string): Promise<Array<string>>;
  bufferWorkflowInputs<T extends any[]>(workflowUUID: string, args: T) : void;
  getWorkflowInputs<T extends any[]>(workflowUUID: string): Promise<T | null>;

  checkOperationOutput<R>(workflowUUID: string, functionID: number): Promise<DBOSNull | R>;
  recordOperationOutput<R>(workflowUUID: string, functionID: number, output: R): Promise<void>;
  recordOperationError(workflowUUID: string, functionID: number, error: Error): Promise<void>;

  getWorkflowStatus(workflowUUID: string, callerUUID?: string, functionID?: number): Promise<WorkflowStatus | null>;
  getWorkflowResult<R>(workflowUUID: string): Promise<R>;

  sleepms(workflowUUID: string, functionID: number, duration: number): Promise<void>;

  send<T extends NonNullable<any>>(workflowUUID: string, functionID: number, destinationUUID: string, message: T, topic?: string): Promise<void>;
  recv<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic?: string, timeoutSeconds?: number): Promise<T | null>;

  setEvent<T extends NonNullable<any>>(workflowUUID: string, functionID: number, key: string, value: T): Promise<void>;
  getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number, callerUUID?: string, functionID?: number): Promise<T | null>;

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
  authenticatedUser: string;
  output: unknown;
  error: string;  // Serialized error
  assumedRole: string;
  authenticatedRoles: string[];
  request: HTTPRequest;
  executorID: string;
  createdAt: number
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
  readonly systemPoolConfig: PoolConfig

  notificationsClient: PoolClient | null = null;
  readonly notificationsMap: Record<string, () => void> = {};
  readonly workflowEventsMap: Record<string, () => void> = {};

  readonly workflowStatusBuffer: Map<string, WorkflowStatusInternal> = new Map();
  readonly workflowInputsBuffer: Map<string, any[]> = new Map();
  readonly flushBatchSize = 100;
  static readonly connectionTimeoutMillis = 10000;  // 10 second timeout

  constructor(readonly pgPoolConfig: PoolConfig, readonly systemDatabaseName: string, readonly logger: Logger) {
    this.systemPoolConfig = { ...pgPoolConfig};
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
    await migrateSystemDatabase(this.systemPoolConfig)
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
    const { rows } = await this.pool.query<workflow_status>(`SELECT status, output, error FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE workflow_uuid=$1`, [workflowUUID]);
    if (rows.length === 0 || rows[0].status === StatusString.PENDING) {
      return dbosNull;
    } else if (rows[0].status === StatusString.ERROR) {
      throw deserializeError(JSON.parse(rows[0].error));
    } else {
      return JSON.parse(rows[0].output) as R;
    }
  }

  async initWorkflowStatus<T extends any[]>(initStatus: WorkflowStatusInternal, args: T): Promise<T> {
    await this.pool.query<workflow_status>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status (workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles, request, output, executor_id, created_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (workflow_uuid) DO NOTHING`,
      [initStatus.workflowUUID, initStatus.status, initStatus.name, initStatus.authenticatedUser, initStatus.assumedRole, JSON.stringify(initStatus.authenticatedRoles), JSON.stringify(initStatus.request), null, initStatus.executorID, initStatus.createdAt]
    );
    const { rows } = await this.pool.query<workflow_inputs>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_inputs (workflow_uuid, inputs) VALUES($1, $2) ON CONFLICT (workflow_uuid) DO UPDATE SET workflow_uuid = excluded.workflow_uuid  RETURNING inputs`,
      [initStatus.workflowUUID, JSON.stringify(args, DBOSReplacer)]
    )
    return JSON.parse(rows[0].inputs, DBOSReviver) as T;
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

  async flushWorkflowStatusBuffer(): Promise<void> {
    const localBuffer = new Map(this.workflowStatusBuffer);
    this.workflowStatusBuffer.clear();
    const totalSize = localBuffer.size;
    try {
      let finishedCnt = 0;
      while (finishedCnt < totalSize) {
        let sqlStmt = `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status (workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles, request, output, executor_id, created_at, updated_at) VALUES `;
        let paramCnt = 1;
        const values: any[] = [];
        const batchUUIDs: string[] = [];
        for (const [workflowUUID, status] of localBuffer) {
          if (paramCnt > 1) {
            sqlStmt += ", ";
          }
          sqlStmt += `($${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++})`;
          values.push(workflowUUID, status.status, status.name, status.authenticatedUser, status.assumedRole, JSON.stringify(status.authenticatedRoles), JSON.stringify(status.request), JSON.stringify(status.output), status.executorID, status.createdAt, Date.now());
          batchUUIDs.push(workflowUUID);
          finishedCnt++;

          if (batchUUIDs.length >= this.flushBatchSize) {
            // Cap at the batch size.
            break;
          }
        }
        sqlStmt += " ON CONFLICT (workflow_uuid) DO UPDATE SET status=EXCLUDED.status, output=EXCLUDED.output, updated_at=EXCLUDED.updated_at;";

        await this.pool.query(sqlStmt, values);

        // Clean up after each batch succeeds
        batchUUIDs.forEach((value) => { localBuffer.delete(value); });
      }
    } catch (error) {
      (error as Error).message = `Error flushing workflow status buffer: ${(error as Error).message}`;
      this.logger.error(error);
    } finally {
      // If there are still items in flushing the buffer, return items to the global buffer for retrying later.
      for (const [workflowUUID, output] of localBuffer) {
        if (!this.workflowStatusBuffer.has(workflowUUID)) {
          this.workflowStatusBuffer.set(workflowUUID, output)
        }
      }
    }
    return;
  }

  async recordWorkflowError(workflowUUID: string, status: WorkflowStatusInternal): Promise<void> {
    await this.pool.query<workflow_status>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status (workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles, request, error, executor_id, created_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (workflow_uuid)
    DO UPDATE SET status=EXCLUDED.status, error=EXCLUDED.error, updated_at=EXCLUDED.updated_at;`,
      [workflowUUID, StatusString.ERROR, status.name, status.authenticatedUser, status.assumedRole, JSON.stringify(status.authenticatedRoles), JSON.stringify(status.request), status.error, status.executorID, status.createdAt, Date.now()]
    );
  }

  async getPendingWorkflows(executorID: string): Promise<Array<string>> {
    const { rows } = await this.pool.query<workflow_status>(
      `SELECT workflow_uuid FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE status=$1 AND executor_id=$2`,
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
          values.push(workflowUUID, JSON.stringify(args, DBOSReplacer));
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
          this.workflowInputsBuffer.set(workflowUUID, args)
        }
      }
    }
    return;
  }

  async getWorkflowInputs<T extends any[]>(workflowUUID: string): Promise<T | null> {
    const { rows } = await this.pool.query<workflow_inputs>(
      `SELECT inputs FROM ${DBOSExecutor.systemDBSchemaName}.workflow_inputs WHERE workflow_uuid=$1`,
      [workflowUUID]
    )
    if (rows.length === 0) {
      return null
    }
    return JSON.parse(rows[0].inputs, DBOSReviver) as T;
  }

  async checkOperationOutput<R>(workflowUUID: string, functionID: number): Promise<DBOSNull | R> {
    const { rows } = await this.pool.query<operation_outputs>(`SELECT output, error FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [workflowUUID, functionID]);
    if (rows.length === 0) {
      return dbosNull;
    } else if (JSON.parse(rows[0].error) !== null) {
      throw deserializeError(JSON.parse(rows[0].error));
    } else {
      return JSON.parse(rows[0].output) as R;
    }
  }

  async recordOperationOutput<R>(workflowUUID: string, functionID: number, output: R): Promise<void> {
    const serialOutput = JSON.stringify(output);
    try {
      await this.pool.query<operation_outputs>(`INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs (workflow_uuid, function_id, output) VALUES ($1, $2, $3);`, [workflowUUID, functionID, serialOutput]);
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
    const serialErr = JSON.stringify(serializeError(error));
    try {
      await this.pool.query<operation_outputs>(`INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs (workflow_uuid, function_id, error) VALUES ($1, $2, $3);`, [workflowUUID, functionID, serialErr]);
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
      await client.query<operation_outputs>(`INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs (workflow_uuid, function_id, output) VALUES ($1, $2, $3);`, [workflowUUID, functionID, JSON.stringify(output)]);
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
    const { rows } = await this.pool.query<operation_outputs>(`SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [workflowUUID, functionID]);
    if (rows.length > 0) {
      const endTimeMs = JSON.parse(rows[0].output) as number;
      await sleepms(Math.max(endTimeMs - Date.now(), 0))
      return;
    } else {
      const endTimeMs = Date.now() + durationMS;
      await this.pool.query<operation_outputs>(`INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs (workflow_uuid, function_id, output) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;`, [workflowUUID, functionID, JSON.stringify(endTimeMs)]);
      await sleepms(Math.max(endTimeMs - Date.now(), 0))
      return;
    }
  }

  readonly nullTopic = "__null__topic__";

  async send<T extends NonNullable<any>>(workflowUUID: string, functionID: number, destinationUUID: string, message: T, topic?: string): Promise<void> {
    topic = topic ?? this.nullTopic;
    const client: PoolClient = await this.pool.connect();

    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    const { rows } = await client.query<operation_outputs>(`SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [workflowUUID, functionID]);
    if (rows.length > 0) {
      await client.query("ROLLBACK");
      client.release();
      return;
    }

    try {
      await client.query(
        `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.notifications (destination_uuid, topic, message) VALUES ($1, $2, $3);`,
        [destinationUUID, topic, JSON.stringify(message)]
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

  async recv<T extends NonNullable<any>>(workflowUUID: string, functionID: number, topic?: string, timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec): Promise<T | null> {
    topic = topic ?? this.nullTopic;
    // First, check for previous executions.
    const checkRows = (await this.pool.query<operation_outputs>(`SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [workflowUUID, functionID])).rows;
    if (checkRows.length > 0) {
      return JSON.parse(checkRows[0].output) as T;
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
    const initRecvRows = (await this.pool.query<notifications>(`SELECT topic FROM ${DBOSExecutor.systemDBSchemaName}.notifications WHERE destination_uuid=$1 AND topic=$2;`, [workflowUUID, topic])).rows;
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
      message = JSON.parse(finalRecvRows[0].message) as T;
    }
    await this.recordNotificationOutput(client, workflowUUID, functionID, message);
    await client.query(`COMMIT`);
    client.release();
    return message;
  }

  async setEvent<T extends NonNullable<any>>(workflowUUID: string, functionID: number, key: string, message: T): Promise<void> {
    const client: PoolClient = await this.pool.connect();

    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    let { rows } = await client.query<operation_outputs>(`SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [workflowUUID, functionID]);
    if (rows.length > 0) {
      await client.query("ROLLBACK");
      client.release();
      return;
    }
    ({ rows } = await client.query(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_events (workflow_uuid, key, value) VALUES ($1, $2, $3) ON CONFLICT (workflow_uuid, key) DO NOTHING RETURNING workflow_uuid;`,
      [workflowUUID, key, JSON.stringify(message)]
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

  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number, callerUUID?: string, functionID?: number): Promise<T | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    if (callerUUID !== undefined && functionID !== undefined) {
      const { rows } = await this.pool.query<operation_outputs>(`SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [callerUUID, functionID]);
      if (rows.length > 0) {
        return JSON.parse(rows[0].output) as T;
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
    const initRecvRows = (await this.pool.query<workflow_events>(`SELECT key, value FROM ${DBOSExecutor.systemDBSchemaName}.workflow_events WHERE workflow_uuid=$1 AND key=$2;`, [workflowUUID, key])).rows;
    if (initRecvRows.length === 0) {
      await received;
    }
    clearTimeout(timer!);

    // Return the value if it's in the DB, otherwise return null.
    let value: T | null = null;
    if (initRecvRows.length > 0) {
      value = JSON.parse(initRecvRows[0].value) as T;
    } else {
      // Read it again from the database.
      const finalRecvRows = (await this.pool.query<workflow_events>(`SELECT value FROM ${DBOSExecutor.systemDBSchemaName}.workflow_events WHERE workflow_uuid=$1 AND key=$2;`, [workflowUUID, key])).rows;
      if (finalRecvRows.length > 0) {
        value = JSON.parse(finalRecvRows[0].value) as T;
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
      const { rows } = await this.pool.query<operation_outputs>(`SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [callerUUID, functionID]);
      if (rows.length > 0) {
        return JSON.parse(rows[0].output) as WorkflowStatus;
      }
    }

    const { rows } = await this.pool.query<workflow_status>(`SELECT status, name, authenticated_user, assumed_role, authenticated_roles, request FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE workflow_uuid=$1`, [workflowUUID]);
    let value = null;
    if (rows.length > 0) {
      value = {
        status: rows[0].status,
        workflowName: rows[0].name,
        authenticatedUser: rows[0].authenticated_user,
        assumedRole: rows[0].assumed_role,
        authenticatedRoles: JSON.parse(rows[0].authenticated_roles) as string[],
        request: JSON.parse(rows[0].request) as HTTPRequest,
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
      const { rows } = await this.pool.query<workflow_status>(`SELECT status, output, error FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE workflow_uuid=$1`, [workflowUUID]);
      if (rows.length > 0) {
        const status = rows[0].status;
        if (status === StatusString.SUCCESS) {
          return JSON.parse(rows[0].output) as R;
        } else if (status === StatusString.ERROR) {
          throw deserializeError(JSON.parse(rows[0].error));
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
    if (v!== null) v = parseInt(`${v}`);
    return v;
  }

  async setLastScheduledTime(wfn: string, invtime: number): Promise<number | null> {
    const res = await this.pool.query<scheduler_state>(`
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.scheduler_state (workflow_fn_name, last_run_time)
      VALUES ($1, $2)
      ON CONFLICT (workflow_fn_name)
      DO UPDATE SET last_run_time = GREATEST(EXCLUDED.last_run_time, scheduler_state.last_run_time)
      RETURNING last_run_time;
    `, [wfn, invtime]);

    return parseInt(`${res.rows[0].last_run_time}`);
  }
}
