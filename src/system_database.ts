/* eslint-disable @typescript-eslint/no-explicit-any */

import { DBOSConfigInternal, DBOSExecutor } from './dbos-executor';
import { DatabaseError, Pool, PoolClient, Notification, PoolConfig, Client } from 'pg';
import {
  DBOSWorkflowConflictUUIDError,
  DBOSNonExistentWorkflowError,
  DBOSDeadLetterQueueError,
  DBOSConflictingWorkflowError,
  DBOSUnexpectedStepError,
} from './error';
import {
  GetPendingWorkflowsOutput,
  GetQueuedWorkflowsInput,
  GetWorkflowQueueInput,
  GetWorkflowQueueOutput,
  GetWorkflowsInput,
  GetWorkflowsOutput,
  StatusString,
  WorkflowStatus,
} from './workflow';
import {
  notifications,
  operation_outputs,
  workflow_status,
  workflow_events,
  workflow_inputs,
  workflow_queue,
  event_dispatch_kv,
} from '../schemas/system_db_schema';
import { sleepms, findPackageRoot, DBOSJSON, globalParams, cancellableSleep } from './utils';
import { HTTPRequest } from './context';
import { GlobalLogger as Logger } from './telemetry/logs';
import knex, { Knex } from 'knex';
import path from 'path';
import { WorkflowQueue } from './wfqueue';
import { DBOSEventReceiverState } from './eventreceiver';

/* Result from Sys DB */
export interface SystemDatabaseStoredResult {
  res?: string | null;
  err?: string | null;
  child?: string | null;
  functionName?: string;
}

export const DBOS_FUNCNAME_SEND = 'DBOS.send';
export const DBOS_FUNCNAME_RECV = 'DBOS.recv';
export const DBOS_FUNCNAME_SETEVENT = 'DBOS.setEvent';
export const DBOS_FUNCNAME_GETEVENT = 'DBOS.getEvent';
export const DBOS_FUNCNAME_SLEEP = 'DBOS.sleep';
export const DBOS_FUNCNAME_GETSTATUS = 'getStatus';

/**
 * General notes:
 *   The responsibilities of the `SystemDatabase` are to store data for workflows, and
 *     associated steps, transactions, messages, and events.  The system DB is
 *     also the IPC mechanism that performs notifications when things change, for
 *     example a receive is unblocked when a send occurs, or a cancel interrupts
 *     the receive.
 *   The `SystemDatabase` expects values in inputs/outputs/errors to be JSON.  However,
 *     the serialization process of turning data into JSON or converting it back, should
 *     be done elsewhere (executor), as it may require application-specific logic or extensions.
 */
export interface SystemDatabase {
  init(): Promise<void>;
  destroy(): Promise<void>;

  initWorkflowStatus<T extends any[]>(
    initStatus: WorkflowStatusInternal,
    args: T,
  ): Promise<{ args: T; status: string }>;
  recordWorkflowOutput(workflowID: string, status: WorkflowStatusInternal): Promise<void>;
  recordWorkflowError(workflowID: string, status: WorkflowStatusInternal): Promise<void>;

  getPendingWorkflows(executorID: string, appVersion: string): Promise<GetPendingWorkflowsOutput[]>;
  getWorkflowInputs<T extends any[]>(workflowID: string): Promise<T | null>;

  // If there is no record, res will be undefined;
  //  otherwise will be defined (with potentially undefined contents)
  getOperationResult(workflowID: string, functionID: number): Promise<{ res?: SystemDatabaseStoredResult }>;
  getAllOperationResults(workflowID: string): Promise<operation_outputs[]>;
  recordOperationResult(
    workflowID: string,
    functionID: number,
    rec: {
      childWfId?: string | null;
      serialOutput?: string | null;
      serialError?: string | null;
      functionName: string;
    },
    checkConflict: boolean,
  ): Promise<void>;

  getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatus | null>;
  getWorkflowStatusInternal(
    workflowID: string,
    callerID?: string,
    callerFN?: number,
  ): Promise<WorkflowStatusInternal | null>;
  awaitWorkflowResult(workflowID: string, timeoutms?: number): Promise<SystemDatabaseStoredResult | undefined>;

  // Workflow management
  setWorkflowStatus(
    workflowID: string,
    status: (typeof StatusString)[keyof typeof StatusString],
    resetRecoveryAttempts: boolean,
  ): Promise<void>;
  cancelWorkflow(workflowID: string): Promise<void>;
  resumeWorkflow(workflowID: string): Promise<void>;

  // Queues
  enqueueWorkflow(workflowId: string, queueName: string): Promise<void>;
  clearQueueAssignment(workflowId: string): Promise<boolean>;
  dequeueWorkflow(workflowId: string, queue: WorkflowQueue): Promise<void>;
  findAndMarkStartableWorkflows(queue: WorkflowQueue, executorID: string, appVersion: string): Promise<string[]>;

  // Actions w/ durable records and notifications
  durableSleepms(
    workflowID: string,
    functionID: number,
    duration: number,
  ): Promise<{ promise: Promise<void>; cancel: () => void }>;

  send(
    workflowID: string,
    functionID: number,
    destinationID: string,
    message: string | null,
    topic?: string,
  ): Promise<void>;
  recv(
    workflowID: string,
    functionID: number,
    timeoutFunctionID: number,
    topic?: string,
    timeoutSeconds?: number,
  ): Promise<string | null>;

  setEvent(workflowID: string, functionID: number, key: string, value: string | null): Promise<void>;
  getEvent(
    workflowID: string,
    key: string,
    timeoutSeconds: number,
    callerWorkflow?: {
      workflowID: string;
      functionID: number;
      timeoutFunctionID: number;
    },
  ): Promise<string | null>;

  // Event receiver state queries / updates
  // An event dispatcher may keep state in the system database
  //   The 'service' should be unique to the event receiver keeping state, to separate from others
  //   The 'workflowFnName' workflow function name should be the fully qualified / unique function name dispatched
  //   The 'key' field allows multiple records per service / workflow function
  //   The service+workflowFnName+key uniquely identifies the record, which is associated with:
  //     'value' - a value set by the event receiver service; this string may be a JSON to keep complex details
  //     A version, either as a sequence number (long integer), or as a time (high precision floating point).
  //       If versions are in use, any upsert is discarded if the version field is less than what is already stored.
  //       The upsert returns the current record, which is useful if it is more recent.
  getEventDispatchState(
    service: string,
    workflowFnName: string,
    key: string,
  ): Promise<DBOSEventReceiverState | undefined>;
  upsertEventDispatchState(state: DBOSEventReceiverState): Promise<DBOSEventReceiverState>;

  // Workflow management
  getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput>;
  getQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<GetWorkflowsOutput>;
  getWorkflowQueue(input: GetWorkflowQueueInput): Promise<GetWorkflowQueueOutput>;
}

// For internal use, not serialized status.
export interface WorkflowStatusInternal {
  workflowUUID: string;
  status: string;
  workflowName: string;
  workflowClassName: string;
  workflowConfigName: string;
  queueName?: string;
  authenticatedUser: string;
  output: string | null;
  error: string | null; // Serialized error
  assumedRole: string;
  authenticatedRoles: string[];
  request: HTTPRequest;
  executorId: string;
  applicationVersion?: string;
  applicationID: string;
  createdAt: number;
  updatedAt?: number;
  maxRetries: number;
  recoveryAttempts?: number;
}

export interface ExistenceCheck {
  exists: boolean;
}

export async function migrateSystemDatabase(systemPoolConfig: PoolConfig, logger: Logger) {
  const migrationsDirectory = path.join(findPackageRoot(__dirname), 'migrations');
  const knexConfig = {
    client: 'pg',
    connection: systemPoolConfig,
    migrations: {
      directory: migrationsDirectory,
      tableName: 'knex_migrations',
    },
  };
  const knexDB = knex(knexConfig);
  try {
    await knexDB.migrate.latest();
  } catch (e) {
    logger.warn(
      `Exception during system database construction. This is most likely because the system database was configured using a later version of DBOS: ${(e as Error).message}`,
    );
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

  static readonly connectionTimeoutMillis = 10000; // 10 second timeout

  constructor(
    readonly pgPoolConfig: PoolConfig,
    readonly systemDatabaseName: string,
    readonly logger: Logger,
    readonly sysDbPoolSize?: number,
  ) {
    this.systemPoolConfig = { ...pgPoolConfig };
    this.systemPoolConfig.database = systemDatabaseName;
    this.systemPoolConfig.connectionTimeoutMillis = PostgresSystemDatabase.connectionTimeoutMillis;
    // This sets the application_name column in pg_stat_activity
    this.systemPoolConfig.application_name = `dbos_transact_${globalParams.executorID}_${globalParams.appVersion}`;
    this.pool = new Pool(this.systemPoolConfig);
    const knexConfig = {
      client: 'pg',
      connection: this.systemPoolConfig,
      pool: {
        min: 0,
        max: this.sysDbPoolSize || 2,
      },
    };
    this.knexDB = knex(knexConfig);
  }

  async init() {
    const pgSystemClient = new Client(this.pgPoolConfig);
    await pgSystemClient.connect();
    // Create the system database and load tables.
    const dbExists = await pgSystemClient.query<ExistenceCheck>(
      `SELECT EXISTS (SELECT FROM pg_database WHERE datname = '${this.systemDatabaseName}')`,
    );
    if (!dbExists.rows[0].exists) {
      // Create the DBOS system database.
      await pgSystemClient.query(`CREATE DATABASE "${this.systemDatabaseName}"`);
    }

    try {
      await migrateSystemDatabase(this.systemPoolConfig, this.logger);
    } catch (e) {
      const tableExists = await this.pool.query<ExistenceCheck>(
        `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'operation_outputs')`,
      );
      if (tableExists.rows[0].exists) {
        this.logger.warn(
          `System database migration failed, you may be running an old version of DBOS Transact: ${(e as Error).message}`,
        );
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

  static async dropSystemDB(dbosConfig: DBOSConfigInternal) {
    // Drop system database, for testing.
    const pgSystemClient = new Client({
      user: dbosConfig.poolConfig.user,
      port: dbosConfig.poolConfig.port,
      host: dbosConfig.poolConfig.host,
      password: dbosConfig.poolConfig.password,
      database: dbosConfig.poolConfig.database,
    });
    await pgSystemClient.connect();
    await pgSystemClient.query(`DROP DATABASE IF EXISTS ${dbosConfig.system_database};`);
    await pgSystemClient.end();
  }

  async initWorkflowStatus<T extends any[]>(
    initStatus: WorkflowStatusInternal,
    args: T,
  ): Promise<{ args: T; status: string }> {
    const result = await this.pool.query<{
      recovery_attempts: number;
      status: string;
      name: string;
      class_name: string;
      config_name: string;
      queue_name?: string;
    }>(
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
        created_at,
        recovery_attempts,
        updated_at
      ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
       ON CONFLICT (workflow_uuid)
        DO UPDATE SET
          recovery_attempts = workflow_status.recovery_attempts + 1,
          updated_at = EXCLUDED.updated_at,
          executor_id = EXCLUDED.executor_id 
        RETURNING recovery_attempts, status, name, class_name, config_name, queue_name`,
      [
        initStatus.workflowUUID,
        initStatus.status,
        initStatus.workflowName,
        initStatus.workflowClassName,
        initStatus.workflowConfigName,
        initStatus.queueName,
        initStatus.authenticatedUser,
        initStatus.assumedRole,
        DBOSJSON.stringify(initStatus.authenticatedRoles),
        DBOSJSON.stringify(initStatus.request),
        null,
        initStatus.executorId,
        initStatus.applicationVersion,
        initStatus.applicationID,
        initStatus.createdAt,
        initStatus.status === StatusString.ENQUEUED ? 0 : 1,
        Date.now(),
      ],
    );
    // Check the started workflow matches the expected name, class_name, config_name, and queue_name
    // A mismatch indicates a workflow starting with the same UUID but different functions, which should not be allowed.
    const resRow = result.rows[0];
    initStatus.workflowConfigName = initStatus.workflowConfigName || '';
    resRow.config_name = resRow.config_name || '';
    resRow.queue_name = resRow.queue_name === null ? undefined : resRow.queue_name; // Convert null in SQL to undefined
    let msg = '';
    if (resRow.name !== initStatus.workflowName) {
      msg = `Workflow already exists with a different function name: ${resRow.name}, but the provided function name is: ${initStatus.workflowName}`;
    } else if (resRow.class_name !== initStatus.workflowClassName) {
      msg = `Workflow already exists with a different class name: ${resRow.class_name}, but the provided class name is: ${initStatus.workflowClassName}`;
    } else if (resRow.config_name !== initStatus.workflowConfigName) {
      msg = `Workflow already exists with a different class configuration: ${resRow.config_name}, but the provided class configuration is: ${initStatus.workflowConfigName}`;
    } else if (resRow.queue_name !== initStatus.queueName) {
      // This is a warning because a different queue name is not necessarily an error.
      this.logger.warn(
        `Workflow (${initStatus.workflowUUID}) already exists in queue: ${resRow.queue_name}, but the provided queue name is: ${initStatus.queueName}. The queue is not updated. ${new Error().stack}`,
      );
    }
    if (msg !== '') {
      throw new DBOSConflictingWorkflowError(initStatus.workflowUUID, msg);
    }

    // recovery_attempt means "attempts" (we kept the name for backward compatibility). It's default value is 1.
    // Every time we init the status, we increment `recovery_attempts` by 1.
    // Thus, when this number becomes equal to `maxRetries + 1`, we should mark the workflow as `RETRIES_EXCEEDED`.
    const attempts = resRow.recovery_attempts;
    if (attempts > initStatus.maxRetries + 1) {
      await this.pool.query(
        `UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status SET status=$1 WHERE workflow_uuid=$2 AND status=$3`,
        [StatusString.RETRIES_EXCEEDED, initStatus.workflowUUID, StatusString.PENDING],
      );
      throw new DBOSDeadLetterQueueError(initStatus.workflowUUID, initStatus.maxRetries);
    }
    this.logger.debug(`Workflow ${initStatus.workflowUUID} attempt number: ${attempts}.`);
    const status = resRow.status;

    const serializedInputs = DBOSJSON.stringify(args);
    const { rows } = await this.pool.query<workflow_inputs>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_inputs (workflow_uuid, inputs) VALUES($1, $2) ON CONFLICT (workflow_uuid) DO UPDATE SET workflow_uuid = excluded.workflow_uuid  RETURNING inputs`,
      [initStatus.workflowUUID, serializedInputs],
    );
    if (serializedInputs !== rows[0].inputs) {
      this.logger.warn(
        `Workflow inputs for ${initStatus.workflowUUID} changed since the first call! Use the original inputs.`,
      );
    }
    return { args: DBOSJSON.parse(rows[0].inputs) as T, status };
  }

  async recordWorkflowStatusChange(
    workflowID: string,
    status: (typeof StatusString)[keyof typeof StatusString],
    update: {
      output?: string | null;
      error?: string | null;
      resetRecoveryAttempts?: boolean;
      incrementRecoveryAttempts?: boolean;
    },
    client?: PoolClient,
  ): Promise<void> {
    let rec = '';
    if (update.resetRecoveryAttempts) {
      rec = ' recovery_attempts = 0, ';
    }
    if (update.incrementRecoveryAttempts) {
      rec = ' recovery_attempts = recovery_attempts + 1';
    }
    const wRes = await (client ?? this.pool).query<workflow_status>(
      `UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status
       SET ${rec} status=$2, output=$3, error=$4, updated_at=$5 WHERE workflow_uuid=$1`,
      [workflowID, status, update.output, update.error, Date.now()],
    );

    if (wRes.rowCount !== 1) {
      throw new DBOSWorkflowConflictUUIDError(`Attempt to record transition of nonexistent workflow ${workflowID}`);
    }
  }

  async recordWorkflowOutput(workflowID: string, status: WorkflowStatusInternal): Promise<void> {
    await this.recordWorkflowStatusChange(workflowID, StatusString.SUCCESS, { output: status.output });
  }

  async recordWorkflowError(workflowID: string, status: WorkflowStatusInternal): Promise<void> {
    await this.recordWorkflowStatusChange(workflowID, StatusString.ERROR, { error: status.error });
  }

  async getPendingWorkflows(executorID: string, appVersion: string): Promise<GetPendingWorkflowsOutput[]> {
    const getWorkflows = await this.pool.query<workflow_status>(
      `SELECT workflow_uuid, queue_name FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE status=$1 AND executor_id=$2 AND application_version=$3`,
      [StatusString.PENDING, executorID, appVersion],
    );
    return getWorkflows.rows.map(
      (i) =>
        <GetPendingWorkflowsOutput>{
          workflowUUID: i.workflow_uuid,
          queueName: i.queue_name,
        },
    );
  }

  async getWorkflowInputs<T extends any[]>(workflowID: string): Promise<T | null> {
    const { rows } = await this.pool.query<workflow_inputs>(
      `SELECT inputs FROM ${DBOSExecutor.systemDBSchemaName}.workflow_inputs WHERE workflow_uuid=$1`,
      [workflowID],
    );
    if (rows.length === 0) {
      return null;
    }
    return DBOSJSON.parse(rows[0].inputs) as T;
  }

  async getOperationResult(
    workflowID: string,
    functionID: number,
    client?: PoolClient,
  ): Promise<{ res?: SystemDatabaseStoredResult }> {
    const { rows } = await (client ?? this.pool).query<operation_outputs>(
      `SELECT output, error, child_workflow_id, function_name
       FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`,
      [workflowID, functionID],
    );
    if (rows.length === 0) {
      return {};
    } else {
      return {
        res: {
          res: rows[0].output,
          err: rows[0].error,
          child: rows[0].child_workflow_id,
          functionName: rows[0].function_name,
        },
      };
    }
  }

  async getAllOperationResults(workflowID: string): Promise<operation_outputs[]> {
    const { rows } = await this.pool.query<operation_outputs>(
      `SELECT * FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1`,
      [workflowID],
    );
    return rows;
  }

  async recordOperationResult(
    workflowID: string,
    functionID: number,
    rec: {
      childWfId?: string | null;
      serialOutput?: string | null;
      serialError?: string | null;
      functionName: string;
    },
    checkConflict: boolean,
    client?: PoolClient,
  ): Promise<void> {
    try {
      await (client ?? this.pool).query<operation_outputs>(
        `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs
          (workflow_uuid, function_id, output, error, function_name, child_workflow_id)
          VALUES ($1, $2, $3, $4, $5, $6)
          ${checkConflict ? '' : ' ON CONFLICT DO NOTHING'}
          ;`,
        [
          workflowID,
          functionID,
          rec.serialOutput ?? null,
          rec.serialError ?? null,
          rec.functionName,
          rec.childWfId ?? null,
        ],
      );
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '40001' || err.code === '23505') {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(workflowID);
      } else {
        throw err;
      }
    }
  }

  async runAsStep(
    callback: () => Promise<string | null | undefined>,
    functionName: string,
    workflowID?: string,
    functionID?: number,
    client?: PoolClient,
  ): Promise<string | null | undefined> {
    if (workflowID !== undefined && functionID !== undefined) {
      const res = await this.getOperationResult(workflowID, functionID, client);
      if (res.res !== undefined) {
        if (res.res.functionName !== functionName) {
          throw new DBOSUnexpectedStepError(workflowID, functionID, functionName, res.res.functionName!);
        }
        await client?.query('ROLLBACK');
        return res.res.res;
      }
    }
    const serialOutput = await callback();
    if (workflowID !== undefined && functionID !== undefined) {
      await this.recordOperationResult(workflowID, functionID, { serialOutput, functionName }, true, client);
    }
    return serialOutput;
  }

  async durableSleepms(
    workflowID: string,
    functionID: number,
    durationMS: number,
  ): Promise<{ promise: Promise<void>; cancel: () => void }> {
    const curTime = Date.now();
    let endTimeMs = curTime + durationMS;
    const res = await this.getOperationResult(workflowID, functionID);
    if (res.res !== undefined) {
      if (res.res.functionName !== DBOS_FUNCNAME_SLEEP) {
        throw new DBOSUnexpectedStepError(workflowID, functionID, DBOS_FUNCNAME_SLEEP, res.res.functionName!);
      }
      endTimeMs = JSON.parse(res.res.res!) as number;
    } else {
      await this.recordOperationResult(
        workflowID,
        functionID,
        { serialOutput: JSON.stringify(endTimeMs), functionName: DBOS_FUNCNAME_SLEEP },
        false,
      );
    }
    return cancellableSleep(Math.max(endTimeMs - curTime, 0));
  }

  readonly nullTopic = '__null__topic__';

  async send(
    workflowID: string,
    functionID: number,
    destinationID: string,
    message: string | null,
    topic?: string,
  ): Promise<void> {
    topic = topic ?? this.nullTopic;
    const client: PoolClient = await this.pool.connect();

    await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
    try {
      await this.runAsStep(
        async () => {
          await client.query(
            `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.notifications (destination_uuid, topic, message) VALUES ($1, $2, $3);`,
            [destinationID, topic, message],
          );
          await client.query('COMMIT');
          return undefined;
        },
        DBOS_FUNCNAME_SEND,
        workflowID,
        functionID,
        client,
      );
    } catch (error) {
      await client.query('ROLLBACK');
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '23503') {
        // Foreign key constraint violation (only expected for the INSERT query)
        throw new DBOSNonExistentWorkflowError(`Sent to non-existent destination workflow UUID: ${destinationID}`);
      } else {
        throw err;
      }
    } finally {
      client.release();
    }
  }

  async recv(
    workflowID: string,
    functionID: number,
    timeoutFunctionID: number,
    topic?: string,
    timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec,
  ): Promise<string | null> {
    topic = topic ?? this.nullTopic;
    // First, check for previous executions.
    const res = await this.getOperationResult(workflowID, functionID);
    if (res.res) {
      if (res.res.functionName !== DBOS_FUNCNAME_RECV) {
        throw new DBOSUnexpectedStepError(workflowID, functionID, DBOS_FUNCNAME_RECV, res.res.functionName!);
      }
      return res.res.res!;
    }

    // Check if the key is already in the DB, then wait for the notification if it isn't.
    const initRecvRows = (
      await this.pool.query<notifications>(
        `SELECT topic FROM ${DBOSExecutor.systemDBSchemaName}.notifications WHERE destination_uuid=$1 AND topic=$2;`,
        [workflowID, topic],
      )
    ).rows;
    if (initRecvRows.length === 0) {
      // Then, register the key with the global notifications listener.
      let resolveNotification: () => void;
      const messagePromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const payload = `${workflowID}::${topic}`;
      this.notificationsMap[payload] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.
      let timeoutPromise: Promise<void> = Promise.resolve();
      let timeoutCancel: () => void = () => {};
      try {
        const { promise, cancel } = await this.durableSleepms(workflowID, timeoutFunctionID, timeoutSeconds * 1000);
        timeoutPromise = promise;
        timeoutCancel = cancel;
      } catch (e) {
        this.logger.error(e);
        delete this.notificationsMap[payload];
        timeoutCancel();
        throw new Error('durable sleepms failed');
      }
      try {
        await Promise.race([messagePromise, timeoutPromise]);
      } finally {
        timeoutCancel();
        delete this.notificationsMap[payload];
      }
    }

    // Transactionally consume and return the message if it's in the DB, otherwise return null.
    let message: string | null = null;
    const client = await this.pool.connect();
    try {
      await client.query(`BEGIN ISOLATION LEVEL READ COMMITTED`);
      const finalRecvRows = (
        await client.query<notifications>(
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
          [workflowID, topic],
        )
      ).rows;
      if (finalRecvRows.length > 0) {
        message = finalRecvRows[0].message;
      }
      await this.recordOperationResult(
        workflowID,
        functionID,
        { serialOutput: message, functionName: DBOS_FUNCNAME_RECV },
        true,
        client,
      );
      await client.query(`COMMIT`);
    } catch (e) {
      this.logger.error(e);
      await client.query(`ROLLBACK`);
      throw e;
    } finally {
      client.release();
    }

    return message;
  }

  async setEvent(workflowID: string, functionID: number, key: string, message: string | null): Promise<void> {
    const client: PoolClient = await this.pool.connect();

    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      await this.runAsStep(
        async () => {
          await client.query(
            `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_events (workflow_uuid, key, value)
             VALUES ($1, $2, $3)
             ON CONFLICT (workflow_uuid, key)
             DO UPDATE SET value = $3
             RETURNING workflow_uuid;`,
            [workflowID, key, message],
          );
          await client.query('COMMIT');
          return undefined;
        },
        DBOS_FUNCNAME_SETEVENT,
        workflowID,
        functionID,
        client,
      );
    } catch (e) {
      this.logger.error(e);
      await client.query(`ROLLBACK`);
      throw e;
    } finally {
      client.release();
    }
  }

  async getEvent(
    workflowID: string,
    key: string,
    timeoutSeconds: number,
    callerWorkflow?: {
      workflowID: string;
      functionID: number;
      timeoutFunctionID: number;
    },
  ): Promise<string | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    if (callerWorkflow) {
      const res = await this.getOperationResult(callerWorkflow.workflowID, callerWorkflow.functionID);
      if (res.res !== undefined) {
        if (res.res.functionName !== DBOS_FUNCNAME_GETEVENT) {
          throw new DBOSUnexpectedStepError(
            callerWorkflow.workflowID,
            callerWorkflow.functionID,
            DBOS_FUNCNAME_GETEVENT,
            res.res.functionName!,
          );
        }
        return res.res.res!;
      }
    }

    // Get the return the value. if it's in the DB, otherwise return null.
    let value: string | null = null;
    const payloadKey = `${workflowID}::${key}`;
    // Register the key with the global notifications listener first... we do not want to look in the DB first
    //  or that would cause a timing hole.
    let resolveNotification: () => void;
    const valuePromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });
    this.workflowEventsMap[payloadKey] = resolveNotification!; // The resolver assignment in the Promise definition runs synchronously.

    try {
      // Check if the key is already in the DB, then wait for the notification if it isn't.
      const initRecvRows = (
        await this.pool.query<workflow_events>(
          `
        SELECT key, value
        FROM ${DBOSExecutor.systemDBSchemaName}.workflow_events
        WHERE workflow_uuid=$1 AND key=$2;`,
          [workflowID, key],
        )
      ).rows;

      if (initRecvRows.length > 0) {
        value = initRecvRows[0].value;
      } else {
        // If we have a callerWorkflow, we want a durable sleep, otherwise, not
        let timeoutPromise: Promise<void> = Promise.resolve();
        let timeoutCancel: () => void = () => {};
        if (callerWorkflow) {
          try {
            const { promise, cancel } = await this.durableSleepms(
              callerWorkflow.workflowID,
              callerWorkflow.timeoutFunctionID ?? -1,
              timeoutSeconds * 1000,
            );
            timeoutPromise = promise;
            timeoutCancel = cancel;
          } catch (e) {
            this.logger.error(e);
            delete this.workflowEventsMap[payloadKey];
            throw new Error('durable sleepms failed');
          }
        } else {
          const { promise, cancel } = cancellableSleep(timeoutSeconds * 1000);
          timeoutPromise = promise;
          timeoutCancel = cancel;
        }

        try {
          await Promise.race([valuePromise, timeoutPromise]);
        } finally {
          timeoutCancel();
        }

        const finalRecvRows = (
          await this.pool.query<workflow_events>(
            `
            SELECT value
            FROM ${DBOSExecutor.systemDBSchemaName}.workflow_events
            WHERE workflow_uuid=$1 AND key=$2;`,
            [workflowID, key],
          )
        ).rows;
        if (finalRecvRows.length > 0) {
          value = finalRecvRows[0].value;
        }
      }
    } finally {
      delete this.workflowEventsMap[payloadKey];
    }

    // Record the output if it is inside a workflow.
    if (callerWorkflow) {
      await this.recordOperationResult(
        callerWorkflow.workflowID,
        callerWorkflow.functionID,
        {
          serialOutput: value,
          functionName: DBOS_FUNCNAME_GETEVENT,
        },
        true,
      );
    }
    return value;
  }

  async setWorkflowStatus(
    workflowID: string,
    status: (typeof StatusString)[keyof typeof StatusString],
    resetRecoveryAttempts: boolean,
  ): Promise<void> {
    await this.recordWorkflowStatusChange(workflowID, status, { resetRecoveryAttempts });
  }

  async cancelWorkflow(workflowID: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Remove workflow from queues table
      await client.query(
        `DELETE FROM ${DBOSExecutor.systemDBSchemaName}.workflow_queue 
         WHERE workflow_uuid = $1`,
        [workflowID],
      );

      // Should we check if it is incomplete first?
      await this.recordWorkflowStatusChange(workflowID, StatusString.CANCELLED, {}, client);

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async resumeWorkflow(workflowID: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Check workflow status. If it is complete, do nothing.
      const statusResult = await client.query<workflow_status>(
        `SELECT status FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status 
         WHERE workflow_uuid = $1`,
        [workflowID],
      );
      if (
        statusResult.rows.length === 0 ||
        statusResult.rows[0].status === StatusString.SUCCESS ||
        statusResult.rows[0].status === StatusString.ERROR
      ) {
        await client.query('COMMIT');
        return;
      }

      // Remove the workflow from the queues table so resume can safely be called on an ENQUEUED workflow
      await client.query(
        `DELETE FROM ${DBOSExecutor.systemDBSchemaName}.workflow_queue 
         WHERE workflow_uuid = $1`,
        [workflowID],
      );

      // Update status to pending and reset recovery attempts
      await this.recordWorkflowStatusChange(workflowID, StatusString.PENDING, { resetRecoveryAttempts: true }, client);

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatus | null> {
    const internalStatus = await this.getWorkflowStatusInternal(workflowID, callerID, callerFN);
    if (internalStatus === null) {
      return null;
    }
    return {
      status: internalStatus.status,
      workflowName: internalStatus.workflowName,
      workflowClassName: internalStatus.workflowClassName,
      workflowConfigName: internalStatus.workflowConfigName,
      queueName: internalStatus.queueName,
      authenticatedUser: internalStatus.authenticatedUser,
      assumedRole: internalStatus.assumedRole,
      authenticatedRoles: internalStatus.authenticatedRoles,
      request: internalStatus.request,
      executorId: internalStatus.executorId,
    };
  }

  async getWorkflowStatusInternal(
    workflowID: string,
    callerID?: string,
    callerFN?: number,
  ): Promise<WorkflowStatusInternal | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    const sv = await this.runAsStep(
      async () => {
        const { rows } = await this.pool.query<workflow_status>(
          `SELECT workflow_uuid, status, name, class_name, config_name, authenticated_user, assumed_role, authenticated_roles, request, queue_name, executor_id, created_at, updated_at, application_version, application_id, recovery_attempts FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE workflow_uuid=$1`,
          [workflowID],
        );

        let value: WorkflowStatusInternal | null = null;
        if (rows.length > 0) {
          value = {
            workflowUUID: rows[0].workflow_uuid,
            status: rows[0].status,
            workflowName: rows[0].name,
            output: null,
            error: null,
            workflowClassName: rows[0].class_name || '',
            workflowConfigName: rows[0].config_name || '',
            queueName: rows[0].queue_name || undefined,
            authenticatedUser: rows[0].authenticated_user,
            assumedRole: rows[0].assumed_role,
            authenticatedRoles: DBOSJSON.parse(rows[0].authenticated_roles) as string[],
            request: DBOSJSON.parse(rows[0].request) as HTTPRequest,
            executorId: rows[0].executor_id,
            createdAt: Number(rows[0].created_at),
            updatedAt: Number(rows[0].updated_at),
            applicationVersion: rows[0].application_version,
            applicationID: rows[0].application_id,
            recoveryAttempts: Number(rows[0].recovery_attempts),
            maxRetries: 0,
          };
        }
        return value ? JSON.stringify(value) : null;
      },
      DBOS_FUNCNAME_GETSTATUS,
      callerID,
      callerFN,
    );

    return sv ? (JSON.parse(sv) as WorkflowStatusInternal) : null;
  }

  async awaitWorkflowResult(workflowID: string, timeoutms?: number): Promise<SystemDatabaseStoredResult | undefined> {
    const pollingIntervalMs: number = 1000;
    const et = timeoutms !== undefined ? new Date().getTime() + timeoutms : undefined;

    while (true) {
      const { rows } = await this.pool.query<workflow_status>(
        `SELECT status, output, error FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE workflow_uuid=$1`,
        [workflowID],
      );
      if (rows.length > 0) {
        const status = rows[0].status;
        if (status === StatusString.SUCCESS) {
          return { res: rows[0].output };
        } else if (status === StatusString.ERROR) {
          return { err: rows[0].error };
        }
      }

      if (et !== undefined) {
        const ct = new Date().getTime();
        if (et > ct) {
          await sleepms(Math.min(pollingIntervalMs, et - ct));
        } else {
          break;
        }
      } else {
        await sleepms(pollingIntervalMs);
      }
    }
    return undefined;
  }

  /* BACKGROUND PROCESSES */
  /**
   * A background process that listens for notifications from Postgres then signals the appropriate
   * workflow listener by resolving its promise.
   */
  async listenForNotifications() {
    this.notificationsClient = await this.pool.connect();
    await this.notificationsClient.query('LISTEN dbos_notifications_channel;');
    await this.notificationsClient.query('LISTEN dbos_workflow_events_channel;');
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
    this.notificationsClient.on('notification', handler);
  }

  // Event dispatcher queries / updates
  async getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSEventReceiverState | undefined> {
    const res = await this.pool.query<event_dispatch_kv>(
      `
      SELECT *
      FROM ${DBOSExecutor.systemDBSchemaName}.event_dispatch_kv
      WHERE workflow_fn_name = $1
      AND service_name = $2
      AND key = $3;
    `,
      [wfn, svc, key],
    );

    if (res.rows.length === 0) return undefined;

    return {
      service: res.rows[0].service_name,
      workflowFnName: res.rows[0].workflow_fn_name,
      key: res.rows[0].key,
      value: res.rows[0].value,
      updateTime: res.rows[0].update_time,
      updateSeq:
        res.rows[0].update_seq !== null && res.rows[0].update_seq !== undefined
          ? BigInt(res.rows[0].update_seq)
          : undefined,
    };
  }

  async upsertEventDispatchState(state: DBOSEventReceiverState): Promise<DBOSEventReceiverState> {
    const res = await this.pool.query<event_dispatch_kv>(
      `
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.event_dispatch_kv (
        service_name, workflow_fn_name, key, value, update_time, update_seq)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (service_name, workflow_fn_name, key)
      DO UPDATE SET
        update_time = GREATEST(EXCLUDED.update_time, event_dispatch_kv.update_time),
        update_seq =  GREATEST(EXCLUDED.update_seq,  event_dispatch_kv.update_seq),
        value = CASE WHEN (EXCLUDED.update_time > event_dispatch_kv.update_time OR EXCLUDED.update_seq > event_dispatch_kv.update_seq OR
                            (event_dispatch_kv.update_time IS NULL and event_dispatch_kv.update_seq IS NULL))
          THEN EXCLUDED.value ELSE event_dispatch_kv.value END
      RETURNING value, update_time, update_seq;
    `,
      [state.service, state.workflowFnName, state.key, state.value, state.updateTime, state.updateSeq],
    );

    return {
      service: state.service,
      workflowFnName: state.workflowFnName,
      key: state.key,
      value: res.rows[0].value,
      updateTime: res.rows[0].update_time,
      updateSeq:
        res.rows[0].update_seq !== undefined && res.rows[0].update_seq !== null
          ? BigInt(res.rows[0].update_seq)
          : undefined,
    };
  }

  async getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput> {
    input.sortDesc = input.sortDesc ?? false; // By default, sort in ascending order
    let query = this.knexDB<{ workflow_uuid: string }>(`${DBOSExecutor.systemDBSchemaName}.workflow_status`).orderBy(
      'created_at',
      input.sortDesc ? 'desc' : 'asc',
    );
    if (input.workflowName) {
      query = query.where('name', input.workflowName);
    }
    if (input.workflowIDs) {
      query = query.whereIn('workflow_uuid', input.workflowIDs);
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
    if (input.offset) {
      query = query.offset(input.offset);
    }
    const rows = await query.select('workflow_uuid');
    const workflowUUIDs = rows.map((row) => row.workflow_uuid);
    return {
      workflowUUIDs: workflowUUIDs,
    };
  }

  async getQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<GetWorkflowsOutput> {
    const sortDesc = input.sortDesc ?? false; // By default, sort in ascending order
    let query = this.knexDB(`${DBOSExecutor.systemDBSchemaName}.workflow_queue`)
      .join(
        `${DBOSExecutor.systemDBSchemaName}.workflow_status`,
        `${DBOSExecutor.systemDBSchemaName}.workflow_queue.workflow_uuid`,
        '=',
        `${DBOSExecutor.systemDBSchemaName}.workflow_status.workflow_uuid`,
      )
      .orderBy(`${DBOSExecutor.systemDBSchemaName}.workflow_status.created_at`, sortDesc ? 'desc' : 'asc');

    if (input.workflowName) {
      query = query.whereRaw(`${DBOSExecutor.systemDBSchemaName}.workflow_status.name = ?`, [input.workflowName]);
    }
    if (input.queueName) {
      query = query.whereRaw(`${DBOSExecutor.systemDBSchemaName}.workflow_status.queue_name = ?`, [input.queueName]);
    }
    if (input.startTime) {
      query = query.where(
        `${DBOSExecutor.systemDBSchemaName}.workflow_status.created_at`,
        '>=',
        new Date(input.startTime).getTime(),
      );
    }
    if (input.endTime) {
      query = query.where(
        `${DBOSExecutor.systemDBSchemaName}.workflow_status.created_at`,
        '<=',
        new Date(input.endTime).getTime(),
      );
    }
    if (input.status) {
      query = query.whereRaw(`${DBOSExecutor.systemDBSchemaName}.workflow_status.status = ?`, [input.status]);
    }
    if (input.limit) {
      query = query.limit(input.limit);
    }
    if (input.offset) {
      query = query.offset(input.offset);
    }

    const rows = await query.select(`${DBOSExecutor.systemDBSchemaName}.workflow_status.workflow_uuid`);
    const workflowUUIDs = rows.map((row) => (row as { workflow_uuid: string }).workflow_uuid);
    return {
      workflowUUIDs: workflowUUIDs,
    };
  }

  async getWorkflowQueue(input: GetWorkflowQueueInput): Promise<GetWorkflowQueueOutput> {
    // Create the initial query with a join to workflow_status table to get executor_id
    let query = this.knexDB(`${DBOSExecutor.systemDBSchemaName}.workflow_queue as wq`)
      .join(`${DBOSExecutor.systemDBSchemaName}.workflow_status as ws`, 'wq.workflow_uuid', '=', 'ws.workflow_uuid')
      .orderBy('wq.created_at_epoch_ms', 'desc');

    if (input.queueName) {
      query = query.where('wq.queue_name', input.queueName);
    }
    if (input.startTime) {
      query = query.where('wq.created_at_epoch_ms', '>=', new Date(input.startTime).getTime());
    }
    if (input.endTime) {
      query = query.where('wq.created_at_epoch_ms', '<=', new Date(input.endTime).getTime());
    }
    if (input.limit) {
      query = query.limit(input.limit);
    }

    const rows = await query
      .select({
        workflow_uuid: 'wq.workflow_uuid',
        executor_id: 'ws.executor_id',
        queue_name: 'wq.queue_name',
        created_at_epoch_ms: 'wq.created_at_epoch_ms',
        started_at_epoch_ms: 'wq.started_at_epoch_ms',
        completed_at_epoch_ms: 'wq.completed_at_epoch_ms',
      })
      .then((rows) => rows as workflow_queue[]);

    const workflows = rows.map((row) => {
      return {
        workflowID: row.workflow_uuid,
        executorID: row.executor_id,
        queueName: row.queue_name,
        createdAt: row.created_at_epoch_ms,
        startedAt: row.started_at_epoch_ms,
        completedAt: row.completed_at_epoch_ms,
      };
    });
    return { workflows };
  }

  async enqueueWorkflow(workflowId: string, queueName: string): Promise<void> {
    await this.pool.query<workflow_queue>(
      `
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_queue (workflow_uuid, queue_name)
      VALUES ($1, $2)
      ON CONFLICT (workflow_uuid)
      DO NOTHING;
    `,
      [workflowId, queueName],
    );
  }

  async clearQueueAssignment(workflowId: string): Promise<boolean> {
    const client: PoolClient = await this.pool.connect();
    try {
      // Reset the start time in the queue to mark it as not started
      const wqRes = await client.query<workflow_queue>(
        `
        UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_queue
        SET started_at_epoch_ms = NULL
        WHERE workflow_uuid = $1 AND completed_at_epoch_ms IS NULL;
      `,
        [workflowId],
      );
      // If no rows were affected, the workflow is not anymore in the queue or was already completed
      if (wqRes.rowCount === 0) {
        await client.query('ROLLBACK');
        return false;
      }
      // Reset the status of the task to "ENQUEUED"
      await this.recordWorkflowStatusChange(workflowId, StatusString.ENQUEUED, {}, client);
      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async dequeueWorkflow(workflowId: string, queue: WorkflowQueue): Promise<void> {
    if (queue.rateLimit) {
      const time = new Date().getTime();
      await this.pool.query<workflow_queue>(
        `
        UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_queue
        SET completed_at_epoch_ms = $2
        WHERE workflow_uuid = $1;
      `,
        [workflowId, time],
      );
    } else {
      await this.pool.query<workflow_queue>(
        `
        DELETE FROM ${DBOSExecutor.systemDBSchemaName}.workflow_queue
        WHERE workflow_uuid = $1;
      `,
        [workflowId],
      );
    }
  }

  async findAndMarkStartableWorkflows(queue: WorkflowQueue, executorID: string, appVersion: string): Promise<string[]> {
    const startTimeMs = new Date().getTime();
    const limiterPeriodMS = queue.rateLimit ? queue.rateLimit.periodSec * 1000 : 0;
    const claimedIDs: string[] = [];

    await this.knexDB.transaction(
      async (trx: Knex.Transaction) => {
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

        // Dequeue functions eligible for this worker and ordered by the time at which they were enqueued.
        // If there is a global or local concurrency limit N, select only the N oldest enqueued
        // functions, else select all of them.

        // First lets figure out how many tasks are eligible for dequeue.
        // This means figuring out how many unstarted tasks are within the local and global concurrency limits
        const runningTasksSubquery = trx(`${DBOSExecutor.systemDBSchemaName}.workflow_queue as wq`)
          .join(`${DBOSExecutor.systemDBSchemaName}.workflow_status as ws`, 'wq.workflow_uuid', '=', 'ws.workflow_uuid')
          .select('ws.executor_id')
          .count('* as task_count')
          .where('wq.queue_name', queue.name)
          .whereNotNull('wq.started_at_epoch_ms') // started
          .whereNull('wq.completed_at_epoch_ms') // not completed
          .groupBy('ws.executor_id');
        const runningTasksResult = await runningTasksSubquery;
        const runningTasksResultDict: Record<string, number> = {};
        runningTasksResult.forEach((row) => {
          runningTasksResultDict[row.executor_id] = Number(row.task_count);
        });
        const runningTasksForThisWorker = runningTasksResultDict[executorID] || 0;

        let maxTasks = Infinity;

        if (queue.workerConcurrency !== undefined) {
          maxTasks = Math.max(0, queue.workerConcurrency - runningTasksForThisWorker);
        }

        if (queue.concurrency !== undefined) {
          const totalRunningTasks = Object.values(runningTasksResultDict).reduce((acc, val) => acc + val, 0);
          if (totalRunningTasks > queue.concurrency) {
            this.logger.warn(
              `Total running tasks (${totalRunningTasks}) exceeds the global concurrency limit (${queue.concurrency})`,
            );
          }
          const availableTasks = Math.max(0, queue.concurrency - totalRunningTasks);
          maxTasks = Math.min(maxTasks, availableTasks);
        }

        // Lookup tasks
        let query = trx<workflow_queue>(`${DBOSExecutor.systemDBSchemaName}.workflow_queue`)
          .whereNull('completed_at_epoch_ms') // not completed
          .whereNull('started_at_epoch_ms') // not started
          .andWhere('queue_name', queue.name)
          .orderBy('created_at_epoch_ms', 'asc')
          .forUpdate()
          .noWait();
        if (maxTasks !== Infinity) {
          query = query.limit(maxTasks);
        }
        const rows = await query.select(['workflow_uuid']);

        // Start the workflows
        const workflowIDs = rows.map((row) => row.workflow_uuid);
        for (const id of workflowIDs) {
          // If we have a rate limit, stop starting functions when the number
          //   of functions started this period exceeds the limit.
          if (queue.rateLimit && numRecentQueries >= queue.rateLimit.limitPerPeriod) {
            break;
          }

          // Start the functions by marking them as pending and updating their executor IDs
          const res = await trx<workflow_status>(`${DBOSExecutor.systemDBSchemaName}.workflow_status`)
            .where('workflow_uuid', id)
            .andWhere('status', StatusString.ENQUEUED)
            .andWhere((b) => {
              b.whereNull('application_version').orWhere('application_version', appVersion);
            })
            .update({
              status: StatusString.PENDING,
              executor_id: executorID,
              application_version: appVersion,
            });

          if (res > 0) {
            claimedIDs.push(id);
            await trx<workflow_queue>(`${DBOSExecutor.systemDBSchemaName}.workflow_queue`)
              .where('workflow_uuid', id)
              .update('started_at_epoch_ms', startTimeMs);
          }
          // If we did not update this record, probably someone else did.  Count in either case.
          ++numRecentQueries;
        }
      },
      { isolationLevel: 'repeatable read' },
    );

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
