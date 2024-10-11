/* eslint-disable @typescript-eslint/no-explicit-any */

import { deserializeError, serializeError } from "serialize-error";
import { DBOSExecutor, dbosNull, DBOSNull } from "./dbos-executor";
import { DatabaseError, Pool, PoolClient, Notification, PoolConfig, Client } from "pg";
import { DBOSWorkflowConflictUUIDError, DBOSNonExistentWorkflowError, DBOSDeadLetterQueueError } from "./error";
import { GetWorkflowQueueInput, GetWorkflowQueueOutput, GetWorkflowsInput, GetWorkflowsOutput, StatusString, WorkflowStatus } from "./workflow";
import {
  notifications,
  operation_outputs,
  workflow_status,
  workflow_events,
  workflow_inputs,
  scheduler_state,
  workflow_queue,
  event_dispatch_kv,
} from "../schemas/system_db_schema";
import { sleepms, findPackageRoot, DBOSJSON } from "./utils";
import { HTTPRequest } from "./context";
import { GlobalLogger as Logger } from "./telemetry/logs";
import knex, { Knex } from "knex";
import path from "path";
import { WorkflowQueue } from "./wfqueue";
import { DBOSEventReceiverQuery, DBOSEventReceiverState } from "./eventreceiver";

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
  setWorkflowStatus(workflowUUID: string, status: typeof StatusString[keyof typeof StatusString], resetRecoveryAttempts: boolean): Promise<void>;

  enqueueWorkflow(workflowId: string, queue: WorkflowQueue): Promise<void>;
  dequeueWorkflow(workflowId: string, queue: WorkflowQueue): Promise<void>;
  findAndMarkStartableWorkflows(queue: WorkflowQueue): Promise<string[]>;

  sleepms(workflowUUID: string, functionID: number, duration: number): Promise<void>;

  send<T>(workflowUUID: string, functionID: number, destinationUUID: string, message: T, topic?: string): Promise<void>;
  recv<T>(workflowUUID: string, functionID: number, timeoutFunctionID: number, topic?: string, timeoutSeconds?: number): Promise<T | null>;

  setEvent<T>(workflowUUID: string, functionID: number, key: string, value: T): Promise<void>;
  getEvent<T>(
    workflowUUID: string,
    key: string,
    timeoutSeconds: number,
    callerWorkflow?: {
      workflowUUID: string,
      functionID: number,
      timeoutFunctionID: number
    }): Promise<T | null>;

  // Scheduler queries
  //  These make sure we kick off the workflow at least once, and wf unique ID does the rest of OAOO
  getLastScheduledTime(wfn: string): Promise<number | null>; // Last workflow we are sure we invoked
  setLastScheduledTime(wfn: string, invtime: number): Promise<number | null>; // We are now sure we invoked another

  // Event dispatcher queries / updates
  getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSEventReceiverState | undefined>;
  queryEventDispatchState(query: DBOSEventReceiverQuery): Promise<DBOSEventReceiverState[]>;
  upsertEventDispatchState(state: DBOSEventReceiverState): Promise<DBOSEventReceiverState>;

  // Workflow management
  getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput>;
  getWorkflowQueue(input: GetWorkflowQueueInput): Promise<GetWorkflowQueueOutput>;
}

// For internal use, not serialized status.
export interface WorkflowStatusInternal {
  workflowUUID: string;
  status: string;
  name: string;
  className: string;
  configName: string;
  queueName?: string;
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
  maxRetries: number;
  recovery: boolean;
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
  const knexDB = knex(knexConfig);
  try {
    await knexDB.migrate.latest();
  } finally {
    await knexDB.destroy();
  }
}

export class PostgresSystemDatabase implements SystemDatabase {
  readonly pool: Pool;
  readonly systemPoolConfig: PoolConfig;
  readonly knexDB: Knex;

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
    const knexConfig = {
      client: 'pg',
      connection: this.systemPoolConfig,
      pool: {
        max: 2
      }
    };
    this.knexDB = knex(knexConfig);
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

    try {
      await migrateSystemDatabase(this.systemPoolConfig);
    } catch (e) {
      const tableExists = await this.pool.query<ExistenceCheck>(`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'operation_outputs')`);
      if (tableExists.rows[0].exists) {
        this.logger.warn(`System database migration failed, you may be running an old version of DBOS Transact: ${(e as Error).message}`);
      } else {
        throw e;
      }
    } finally {
      await pgSystemClient.end();
    }
    await this.listenForNotifications();
  }

  async destroy() {
    await this.knexDB.destroy();
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
      throw deserializeError(DBOSJSON.parse(rows[0].error));
    } else {
      return DBOSJSON.parse(rows[0].output) as R;
    }
  }

  async initWorkflowStatus<T extends any[]>(initStatus: WorkflowStatusInternal, args: T): Promise<T> {
    const result = await this.pool.query<{recovery_attempts: number}>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status (
        workflow_uuid,
        status,
        name,
        class_name,
        config_name,
        queue_name,
        authenticated_user,
        assumed_role,
        authenticated_roles,
        request,
        output,
        executor_id,
        application_version,
        application_id,
        created_at
      ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
       ON CONFLICT (workflow_uuid)
        DO UPDATE SET
          recovery_attempts = CASE WHEN $16 THEN workflow_status.recovery_attempts + 1 ELSE workflow_status.recovery_attempts END
        RETURNING recovery_attempts`,
      [
        initStatus.workflowUUID,
        initStatus.status,
        initStatus.name,
        initStatus.className,
        initStatus.configName,
        initStatus.queueName,
        initStatus.authenticatedUser,
        initStatus.assumedRole,
        DBOSJSON.stringify(initStatus.authenticatedRoles),
        DBOSJSON.stringify(initStatus.request),
        null,
        initStatus.executorID,
        initStatus.applicationVersion,
        initStatus.applicationID,
        initStatus.createdAt,
        initStatus.recovery,
      ]
    );
    const recovery_attempts = result.rows[0].recovery_attempts;
    if (recovery_attempts >= initStatus.maxRetries && initStatus.recovery) {
      await this.pool.query(`UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status SET status=$1 WHERE workflow_uuid=$2 AND status=$3`, [StatusString.RETRIES_EXCEEDED, initStatus.workflowUUID, StatusString.PENDING]);
      throw new DBOSDeadLetterQueueError(initStatus.workflowUUID, initStatus.maxRetries);
    }

    const { rows } = await this.pool.query<workflow_inputs>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_inputs (workflow_uuid, inputs) VALUES($1, $2) ON CONFLICT (workflow_uuid) DO UPDATE SET workflow_uuid = excluded.workflow_uuid  RETURNING inputs`,
      [initStatus.workflowUUID, DBOSJSON.stringify(args)]
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

  async flushWorkflowStatusBuffer(): Promise<void> {
    const localBuffer = new Map(this.workflowStatusBuffer);
    this.workflowStatusBuffer.clear();
    const totalSize = localBuffer.size;
    try {
      let finishedCnt = 0;
      while (finishedCnt < totalSize) {
        let sqlStmt = `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status (workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles, request, output, executor_id, application_version, application_id, created_at, updated_at, class_name, config_name, queue_name) VALUES `;
        let paramCnt = 1;
        const values: any[] = [];
        const batchUUIDs: string[] = [];
        for (const [workflowUUID, status] of localBuffer) {
          if (paramCnt > 1) {
            sqlStmt += ", ";
          }
          sqlStmt += `($${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++})`;
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
            Date.now(),
            status.className,
            status.configName,
            status.queueName,
          );
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
        batchUUIDs.forEach((value) => {
          localBuffer.delete(value);
        });
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
    await this.pool.query<workflow_status>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status (
        workflow_uuid,
        status,
        name,
        class_name,
        config_name,
        queue_name,
        authenticated_user,
        assumed_role,
        authenticated_roles,
        request,
        error,
        executor_id,
        application_id,
        application_version,
        created_at,
        updated_at
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
    ON CONFLICT (workflow_uuid)
    DO UPDATE SET status=EXCLUDED.status, error=EXCLUDED.error, updated_at=EXCLUDED.updated_at;`,
      [
        workflowUUID,
        StatusString.ERROR,
        status.name,
        status.className,
        status.configName,
        status.queueName,
        status.authenticatedUser,
        status.assumedRole,
        DBOSJSON.stringify(status.authenticatedRoles),
        DBOSJSON.stringify(status.request),
        status.error,
        status.executorID,
        status.applicationID,
        status.applicationVersion,
        status.createdAt,
        Date.now(),
      ]
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
    const { rows } = await this.pool.query<workflow_inputs>(
      `SELECT inputs FROM ${DBOSExecutor.systemDBSchemaName}.workflow_inputs WHERE workflow_uuid=$1`,
      [workflowUUID]
    )
    if (rows.length === 0) {
      return null
    }
    return DBOSJSON.parse(rows[0].inputs) as T;
  }

  async checkOperationOutput<R>(workflowUUID: string, functionID: number): Promise<DBOSNull | R> {
    const { rows } = await this.pool.query<operation_outputs>(`SELECT output, error FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [workflowUUID, functionID]);
    if (rows.length === 0) {
      return dbosNull;
    } else if (DBOSJSON.parse(rows[0].error) !== null) {
      throw deserializeError(DBOSJSON.parse(rows[0].error));
    } else {
      return DBOSJSON.parse(rows[0].output) as R;
    }
  }

  async recordOperationOutput<R>(workflowUUID: string, functionID: number, output: R): Promise<void> {
    const serialOutput = DBOSJSON.stringify(output);
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
    const serialErr = DBOSJSON.stringify(serializeError(error));
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
      await client.query<operation_outputs>(`INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs (workflow_uuid, function_id, output) VALUES ($1, $2, $3);`, [workflowUUID, functionID, DBOSJSON.stringify(output)]);
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
      const endTimeMs = DBOSJSON.parse(rows[0].output) as number;
      await sleepms(Math.max(endTimeMs - Date.now(), 0))
      return;
    } else {
      const endTimeMs = Date.now() + durationMS;
      await this.pool.query<operation_outputs>(`INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs (workflow_uuid, function_id, output) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;`, [workflowUUID, functionID, DBOSJSON.stringify(endTimeMs)]);
      await sleepms(Math.max(endTimeMs - Date.now(), 0))
      return;
    }
  }

  readonly nullTopic = "__null__topic__";

  async send<T>(workflowUUID: string, functionID: number, destinationUUID: string, message: T, topic?: string): Promise<void> {
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
        [destinationUUID, topic, DBOSJSON.stringify(message)]
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

  async recv<T>(
    workflowUUID: string,
    functionID: number,
    timeoutFunctionID: number,
    topic?: string,
    timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec
  ): Promise<T | null> {
    topic = topic ?? this.nullTopic;
    // First, check for previous executions.
    const checkRows = (await this.pool.query<operation_outputs>(`SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [workflowUUID, functionID])).rows;
    if (checkRows.length > 0) {
      return DBOSJSON.parse(checkRows[0].output) as T;
    }

    // Check if the key is already in the DB, then wait for the notification if it isn't.
    const initRecvRows = (await this.pool.query<notifications>(`SELECT topic FROM ${DBOSExecutor.systemDBSchemaName}.notifications WHERE destination_uuid=$1 AND topic=$2;`, [workflowUUID, topic])).rows;
    if (initRecvRows.length === 0) {
      // Then, register the key with the global notifications listener.
      let resolveNotification: () => void;
      const messagePromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const payload = `${workflowUUID}::${topic}`;
      this.notificationsMap[payload] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.
      let timer: NodeJS.Timeout;
      const timeoutPromise = new Promise<void>(async (resolve, reject) => {
        try {
          await this.sleepms(workflowUUID, timeoutFunctionID, timeoutSeconds * 1000);
          resolve();
        } catch (e) {
          this.logger.error(e);
          reject(new Error('sleepms failed'));
        }
      });
      try {
        await Promise.race([messagePromise, timeoutPromise])
      } finally {
        clearTimeout(timer!);
        delete this.notificationsMap[payload];
      }
    }

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
    let { rows } = await client.query<operation_outputs>(`SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [workflowUUID, functionID]);
    if (rows.length > 0) {
      await client.query("ROLLBACK");
      client.release();
      return;
    }
    ({ rows } = await client.query(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_events (workflow_uuid, key, value)
       VALUES ($1, $2, $3)
       ON CONFLICT (workflow_uuid, key)
       DO UPDATE SET value = $3
       RETURNING workflow_uuid;`,
      [workflowUUID, key, DBOSJSON.stringify(message)]
    ));
    await this.recordNotificationOutput(client, workflowUUID, functionID, undefined);
    await client.query("COMMIT");
    client.release();
  }

  async getEvent<T>(
    workflowUUID: string,
    key: string,
    timeoutSeconds: number,
    callerWorkflow?: {
      workflowUUID: string,
      functionID: number,
      timeoutFunctionID: number
    }): Promise<T | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    if (callerWorkflow) {
      const { rows } = await this.pool.query<operation_outputs>(`
        SELECT output
        FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
        WHERE workflow_uuid=$1 AND function_id=$2`,
        [callerWorkflow.workflowUUID, callerWorkflow.functionID]);
      if (rows.length > 0) {
        return DBOSJSON.parse(rows[0].output) as T;
      }
    }

    // Check if the key is already in the DB, then wait for the notification if it isn't.
    const initRecvRows = (await this.pool.query<workflow_events>(`
      SELECT key, value
      FROM ${DBOSExecutor.systemDBSchemaName}.workflow_events
      WHERE workflow_uuid=$1 AND key=$2;`,
      [workflowUUID, key])).rows;

    // Return the value if it's in the DB, otherwise return null.
    let value: T | null = null;
    if (initRecvRows.length > 0) {
      value = DBOSJSON.parse(initRecvRows[0].value) as T;
    } else {
      // Register the key with the global notifications listener.
      let resolveNotification: () => void;
      const valuePromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const payload = `${workflowUUID}::${key}`;
      this.workflowEventsMap[payload] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.
      let timer: NodeJS.Timeout;
      const timeoutMillis = timeoutSeconds * 1000;
      const timeoutPromise = callerWorkflow
        ? new Promise<void>(async (resolve, reject) => {
            try {
              await this.sleepms(callerWorkflow.workflowUUID, callerWorkflow.timeoutFunctionID, timeoutMillis);
              resolve();
            } catch (e) {
              this.logger.error(e);
              reject(new Error('sleepms failed'));
            }
          })
        : new Promise<void>((resolve) => {
            timer = setTimeout(() => {
              resolve();
            }, timeoutMillis);
          });

      try {
        await Promise.race([valuePromise, timeoutPromise]);
      } finally {
        clearTimeout(timer!);
        delete this.workflowEventsMap[payload];
      }
      const finalRecvRows = (await this.pool.query<workflow_events>(`
          SELECT value
          FROM ${DBOSExecutor.systemDBSchemaName}.workflow_events
          WHERE workflow_uuid=$1 AND key=$2;`,
          [workflowUUID, key])).rows;
        if (finalRecvRows.length > 0) {
          value = DBOSJSON.parse(finalRecvRows[0].value) as T;
        }
    }

    // Record the output if it is inside a workflow.
    if (callerWorkflow) {
      await this.recordOperationOutput(callerWorkflow.workflowUUID, callerWorkflow.functionID, value);
    }
    return value;
  }

  async setWorkflowStatus(workflowUUID: string, status: typeof StatusString[keyof typeof StatusString], resetRecoveryAttempts: boolean): Promise<void> {
    await this.pool.query(`UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status SET status=$1 WHERE workflow_uuid=$2`, [status, workflowUUID]);
    if (resetRecoveryAttempts) {
      await this.pool.query(`UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status SET recovery_attempts=0 WHERE workflow_uuid=$1`, [workflowUUID]);
    }
  }

  async getWorkflowStatus(workflowUUID: string, callerUUID?: string, functionID?: number): Promise<WorkflowStatus | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    if (callerUUID !== undefined && functionID !== undefined) {
      const { rows } = await this.pool.query<operation_outputs>(`SELECT output FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1 AND function_id=$2`, [callerUUID, functionID]);
      if (rows.length > 0) {
        return DBOSJSON.parse(rows[0].output) as WorkflowStatus;
      }
    }

    const { rows } = await this.pool.query<workflow_status>(`SELECT status, name, class_name, config_name, authenticated_user, assumed_role, authenticated_roles, request, queue_name FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE workflow_uuid=$1`, [workflowUUID]);
    let value = null;
    if (rows.length > 0) {
      value = {
        status: rows[0].status,
        workflowName: rows[0].name,
        workflowClassName: rows[0].class_name || "",
        workflowConfigName: rows[0].config_name || "",
        queueName: rows[0].queue_name || undefined,
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

    while (true) {
      const { rows } = await this.pool.query<workflow_status>(`SELECT status, output, error FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE workflow_uuid=$1`, [workflowUUID]);
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
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.scheduler_state (workflow_fn_name, last_run_time)
      VALUES ($1, $2)
      ON CONFLICT (workflow_fn_name)
      DO UPDATE SET last_run_time = GREATEST(EXCLUDED.last_run_time, scheduler_state.last_run_time)
      RETURNING last_run_time;
    `, [wfn, invtime]);

    return parseInt(`${res.rows[0].last_run_time}`);
  }

  // Event dispatcher queries / updates
  async getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSEventReceiverState | undefined> {
    const res = await this.pool.query<event_dispatch_kv>(`
      SELECT *
      FROM ${DBOSExecutor.systemDBSchemaName}.event_dispatch_kv
      WHERE workflow_fn_name = $1
      AND service_name = $2
      AND key = $3;
    `, [wfn, svc, key]);

    if (res.rows.length === 0) return undefined;

    return {
      service: res.rows[0].service_name,
      workflowFnName: res.rows[0].workflow_fn_name,
      key: res.rows[0].key,
      value: res.rows[0].value,
      updateTime: res.rows[0].update_time,
      updateSeq: res.rows[0].update_seq,
    };
  }

  async queryEventDispatchState(input: DBOSEventReceiverQuery): Promise<DBOSEventReceiverState[]> {
    let query = this.knexDB<event_dispatch_kv>(`${DBOSExecutor.systemDBSchemaName}.event_dispatch_kv`);
    if (input.service) {
      query = query.where('service_name', input.service);
    }
    if (input.workflowFnName) {
      query = query.where('workflow_fn_name', input.workflowFnName);
    }
    if (input.key) {
      query = query.where('key', input.key);
    }
    if (input.startTime) {
      query = query.where('update_time', '>=', new Date(input.startTime).getTime());
    }
    if (input.endTime) {
      query = query.where('update_time', '<=', new Date(input.endTime).getTime());
    }
    if (input.startSeq) {
      query = query.where('update_seq', '>=', input.startSeq);
    }
    if (input.endSeq) {
      query = query.where('update_seq', '<=', input.endSeq);
    }
    const rows = await query.select();
    const ers = rows.map((row) => { return {
      service: row.service_name,
      workflowFnName: row.workflow_fn_name,
      key: row.key,
      value: row.value,
      updateTime: row.update_time,
      updateSeq: row.update_seq,
    }});
    return ers;
  }

  async upsertEventDispatchState(state: DBOSEventReceiverState): Promise<DBOSEventReceiverState> {
    const res = await this.pool.query<event_dispatch_kv>(`
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.event_dispatch_kv (
        service_name, workflow_fn_name, key, value, update_time, update_seq)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (service_name, workflow_fn_name, key)
      DO UPDATE SET
        update_time = GREATEST(EXCLUDED.update_time, event_dispatch_kv.update_time),
        update_seq =  GREATEST(EXCLUDED.update_seq,  event_dispatch_kv.update_seq),
        value = CASE WHEN (EXCLUDED.update_time > event_dispatch_kv.update_time OR EXCLUDED.update_seq > event_dispatch_kv.update_seq)
          THEN EXCLUDED.value ELSE event_dispatch_kv.value END
      RETURNING value, update_time, update_seq;
    `, [
      state.service,
      state.workflowFnName,
      state.key,
      state.value,
      state.updateTime,
      state.updateSeq,
    ]);

    return {
      service: state.service,
      workflowFnName: state.workflowFnName,
      key: state.key,
      value: res.rows[0].value,
      updateTime: res.rows[0].update_time,
      updateSeq: res.rows[0].update_seq,
    };
  }
  
  async getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput> {
    let query = this.knexDB<{workflow_uuid: string}>(`${DBOSExecutor.systemDBSchemaName}.workflow_status`).orderBy('created_at', 'desc');
    if (input.workflowName) {
      query = query.where('name', input.workflowName);
    }
    if (input.authenticatedUser) {
      query = query.where('authenticated_user', input.authenticatedUser);
    }
    if (input.startTime) {
      query = query.where('created_at', '>=', new Date(input.startTime).getTime());
    }
    if (input.endTime) {
      query = query.where('created_at', '<=', new Date(input.endTime).getTime());
    }
    if (input.status) {
      query = query.where('status', input.status);
    }
    if (input.applicationVersion) {
      query = query.where('application_version', input.applicationVersion);
    }
    if (input.limit) {
      query = query.limit(input.limit);
    }
    const rows = await query.select('workflow_uuid');
    const workflowUUIDs = rows.map(row => row.workflow_uuid);
    return {
      workflowUUIDs: workflowUUIDs
    };
  }

  async getWorkflowQueue(input: GetWorkflowQueueInput): Promise<GetWorkflowQueueOutput> {
    let query = this.knexDB<workflow_queue>(`${DBOSExecutor.systemDBSchemaName}.workflow_queue`).orderBy('created_at_epoch_ms', 'desc');
    if (input.queueName) {
      query = query.where('queue_name', input.queueName);
    }
    if (input.startTime) {
      query = query.where('created_at_epoch_ms', '>=', new Date(input.startTime).getTime());
    }
    if (input.endTime) {
      query = query.where('created_at_at_epoch_ms', '<=', new Date(input.endTime).getTime());
    }
    if (input.limit) {
      query = query.limit(input.limit);
    }
    const rows = await query.select();
    const workflows = rows.map((row) => { return {
      workflowID: row.workflow_uuid,
      queueName: row.queue_name,
      createdAt: row.created_at_epoch_ms,
      startedAt: row.started_at_epoch_ms,
      completedAt: row.completed_at_epoch_ms,
    }});
    return { workflows };
  }

  async enqueueWorkflow(workflowId: string, queue: WorkflowQueue): Promise<void> {
    const _res = await this.pool.query<scheduler_state>(`
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_queue (workflow_uuid, queue_name)
      VALUES ($1, $2)
      ON CONFLICT (workflow_uuid)
      DO NOTHING;
    `, [workflowId, queue.name]);
  }

  async dequeueWorkflow(workflowId: string, queue: WorkflowQueue): Promise<void> {
    if (queue.rateLimit) {
      const time = new Date().getTime();
      const _res = await this.pool.query<workflow_queue>(`
        UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_queue
        SET completed_at_epoch_ms = $2
        WHERE workflow_uuid = $1;
      `, [workflowId, time]);

    }
    else {
      const _res = await this.pool.query<workflow_queue>(`
        DELETE FROM ${DBOSExecutor.systemDBSchemaName}.workflow_queue
        WHERE workflow_uuid = $1;
      `, [workflowId]);
    }
  }
  
  async  findAndMarkStartableWorkflows(queue: WorkflowQueue): Promise<string[]> {
    const startTimeMs = new Date().getTime();
    const limiterPeriodMS = queue.rateLimit ? queue.rateLimit.periodSec * 1000 : 0;
    const claimedIDs: string[] = [];

    await this.knexDB.transaction(async (trx: Knex.Transaction) => {
      // If there is a rate limit, compute how many functions have started in its period.
      let numRecentQueries = 0;
      if (queue.rateLimit) {
        const numRecentQueriesS = (await trx(`${DBOSExecutor.systemDBSchemaName}.workflow_queue`)
          .count()
          .where('queue_name', queue.name)
          .andWhere('started_at_epoch_ms', '>', startTimeMs - limiterPeriodMS)
          .first())!.count;
        numRecentQueries = parseInt(`${numRecentQueriesS}`);
        if (numRecentQueries >= queue.rateLimit.limitPerPeriod) {
          return claimedIDs;
        }
      }

      // Select not-yet-completed functions in the queue ordered by the
      //   time at which they were enqueued.
      // If there is a concurrency limit N, select only the N most recent
      //   functions, else select all of them.
      // Started functions count toward concurrency, will be filtered below
      let query = trx<workflow_queue>(`${DBOSExecutor.systemDBSchemaName}.workflow_queue`)
        .whereNull('completed_at_epoch_ms')
        .andWhere('queue_name', queue.name)
        .select();
      query = query.orderBy('created_at_epoch_ms', 'asc');
      if (queue.concurrency !== undefined) {
        query = query.limit(queue.concurrency);
      }

      // From the functions retrieved, get the workflow IDs of the functions
      // that have not yet been started so we can start them.
      const rows = await query.select(['workflow_uuid', 'started_at_epoch_ms']);
      const workflowIDs = rows
        .filter((row) => !row.started_at_epoch_ms)
        .map(row => row.workflow_uuid);
      for (const id of workflowIDs) {
        // If we have a rate limit, stop starting functions when the number
        //   of functions started this period exceeds the limit.
        if (queue.rateLimit && numRecentQueries >= queue.rateLimit.limitPerPeriod) {
          break;
        }
        const res = await trx<workflow_status>(`${DBOSExecutor.systemDBSchemaName}.workflow_status`)
          .where('workflow_uuid', id)
          .andWhere('status', StatusString.ENQUEUED)
          .update('status', StatusString.PENDING);
        if (res > 0) {
          claimedIDs.push(id);
          await trx<workflow_queue>(`${DBOSExecutor.systemDBSchemaName}.workflow_queue`)
            .where('workflow_uuid', id)
            .update('started_at_epoch_ms', startTimeMs);
        }
        // If we did not update this record, probably someone else did.  Count in either case.
        ++numRecentQueries;
      }  
    }, {isolationLevel: "repeatable read"});

    // If we have a rate limit, garbage-collect all completed functions started
    //   before the period. If there's no limiter, there's no need--they were
    //   deleted on completion.
    if (queue.rateLimit) {
      await this.knexDB<workflow_queue>(`${DBOSExecutor.systemDBSchemaName}.workflow_queue`)
        .whereNotNull('completed_at_epoch_ms')
        .andWhere('queue_name', queue.name)
        .andWhere('started_at_epoch_ms', '<', startTimeMs - limiterPeriodMS)
        .delete();
    }

    // Return the IDs of all functions we marked started
    return claimedIDs;
  }
}
