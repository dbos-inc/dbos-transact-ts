import { DBOSConfigInternal, DBOSExecutor } from './dbos-executor';
import { DatabaseError, Pool, PoolClient, Notification, PoolConfig, Client } from 'pg';
import {
  DBOSWorkflowConflictError,
  DBOSNonExistentWorkflowError,
  DBOSDeadLetterQueueError,
  DBOSConflictingWorkflowError,
  DBOSUnexpectedStepError,
  DBOSWorkflowCancelledError,
} from './error';
import {
  GetPendingWorkflowsOutput,
  GetQueuedWorkflowsInput,
  GetWorkflowQueueInput,
  GetWorkflowQueueOutput,
  GetWorkflowsInput,
  StatusString,
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
import { findPackageRoot, globalParams, cancellableSleep, INTERNAL_QUEUE_NAME } from './utils';
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
  cancelled?: boolean;
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

  initWorkflowStatus(
    initStatus: WorkflowStatusInternal,
    serializedArgs: string,
    maxRetries?: number,
  ): Promise<{ serializedInputs: string; status: string }>;
  recordWorkflowOutput(workflowID: string, status: WorkflowStatusInternal): Promise<void>;
  recordWorkflowError(workflowID: string, status: WorkflowStatusInternal): Promise<void>;

  getPendingWorkflows(executorID: string, appVersion: string): Promise<GetPendingWorkflowsOutput[]>;
  getWorkflowInputs(workflowID: string): Promise<string | null>;

  // If there is no record, res will be undefined;
  //  otherwise will be defined (with potentially undefined contents)
  getOperationResultAndThrowIfCancelled(
    workflowID: string,
    functionID: number,
  ): Promise<{ res?: SystemDatabaseStoredResult }>;
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

  getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatusInternal | null>;
  awaitWorkflowResult(
    workflowID: string,
    timeoutSeconds?: number,
    callerID?: string,
    timerFuncID?: number,
  ): Promise<SystemDatabaseStoredResult | undefined>;

  // Workflow management
  setWorkflowStatus(
    workflowID: string,
    status: (typeof StatusString)[keyof typeof StatusString],
    resetRecoveryAttempts: boolean,
  ): Promise<void>;
  cancelWorkflow(workflowID: string): Promise<void>;
  resumeWorkflow(workflowID: string): Promise<void>;
  forkWorkflow(originalWorkflowID: string, forkedWorkflowId: string, startStep: number): Promise<string>;
  getMaxFunctionID(workflowID: string): Promise<number>;
  checkIfCanceled(workflowID: string): Promise<void>;
  registerRunningWorkflow(workflowID: string, workflowPromise: Promise<unknown>): void;
  awaitRunningWorkflows(): Promise<void>; // Use in clean shutdown

  // Queues
  enqueueWorkflow(workflowId: string, queueName: string): Promise<void>;
  clearQueueAssignment(workflowId: string): Promise<boolean>;
  dequeueWorkflow(workflowId: string, queue: WorkflowQueue): Promise<void>;
  findAndMarkStartableWorkflows(queue: WorkflowQueue, executorID: string, appVersion: string): Promise<string[]>;

  // Actions w/ durable records and notifications
  durableSleepms(workflowID: string, functionID: number, duration: number): Promise<void>;

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
  listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatusInternal[]>;
  listQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<WorkflowStatusInternal[]>;

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
  input?: string;
  assumedRole: string;
  authenticatedRoles: string[];
  request: HTTPRequest;
  executorId: string;
  applicationVersion?: string;
  applicationID: string;
  createdAt: number;
  updatedAt?: number;
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

class NotificationMap<T> {
  map: Map<string, Map<number, (event?: T) => void>> = new Map();
  curCK: number = 0;

  registerCallback(key: string, cb: (event?: T) => void) {
    if (!this.map.has(key)) {
      this.map.set(key, new Map());
    }
    const ck = this.curCK++;
    this.map.get(key)!.set(ck, cb);
    return { key, ck };
  }

  deregisterCallback(k: { key: string; ck: number }) {
    if (!this.map.has(k.key)) return;
    const sm = this.map.get(k.key)!;
    if (!sm.has(k.ck)) return;
    sm.delete(k.ck);
    if (sm.size === 0) {
      this.map.delete(k.key);
    }
  }

  callCallbacks(key: string, event?: T) {
    if (!this.map.has(key)) return;
    const sm = this.map.get(key)!;
    for (const cb of sm.values()) {
      cb(event);
    }
  }
}

export class PostgresSystemDatabase implements SystemDatabase {
  readonly pool: Pool;
  readonly systemPoolConfig: PoolConfig;
  readonly knexDB: Knex;

  /*
   * Generally, notifications are asynchronous.  One should:
   *  Subscribe to updates
   *  Read the database item in question
   *  In response to updates, re-read the database item
   *  Unsubscribe at the end
   * The notification mechanism is reliable in the sense that it will eventually deliver updates
   *  or the DB connection will get dropped.  The right thing to do if you lose connectivity to
   *  the system DB is to exit the process and go through recovery... system DB writes, notifications,
   *  etc may not have completed correctly, and recovery is the way to rebuild in-memory state.
   *
   * NOTE:
   * PG Notifications are not fully reliable.
   *   Dropped connections are recoverable - you just need to restart and scan everything.
   *      (The whole VM being the logical choice, so workflows can recover from any write failures.)
   *   The real problem is, if the pipes out of the server are full... then notifications can be
   *     dropped, and only the PG server log may note it.  For those reasons, we do occasional polling
   */
  notificationsClient: PoolClient | null = null;
  dbPollingIntervalResultMs: number = 1000;
  dbPollingIntervalEventMs: number = 10000;
  shouldUseDBNotifications: boolean = true;
  readonly notificationsMap: NotificationMap<void> = new NotificationMap();
  readonly workflowEventsMap: NotificationMap<void> = new NotificationMap();
  readonly cancelWakeupMap: NotificationMap<void> = new NotificationMap();

  readonly runningWorkflowMap: Map<string, Promise<unknown>> = new Map(); // Map from workflowID to workflow promise
  readonly workflowCancellationMap: Map<string, boolean> = new Map(); // Map from workflowID to its cancellation status.

  constructor(
    readonly pgPoolConfig: PoolConfig,
    readonly systemDatabaseName: string,
    readonly logger: Logger,
    readonly sysDbPoolSize?: number,
  ) {
    // Craft a db string from the app db string, replacing the database name:
    const systemDbConnectionString = new URL(pgPoolConfig.connectionString!);
    systemDbConnectionString.pathname = `/${systemDatabaseName}`;

    this.systemPoolConfig = {
      connectionString: systemDbConnectionString.toString(),
      connectionTimeoutMillis: pgPoolConfig.connectionTimeoutMillis,
      // This sets the application_name column in pg_stat_activity
      application_name: `dbos_transact_${globalParams.executorID}_${globalParams.appVersion}`,
    };
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
    if (this.shouldUseDBNotifications) {
      await this.listenForNotifications();
    }
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

  async initWorkflowStatus(
    initStatus: WorkflowStatusInternal,
    serializedInputs: string,
    maxRetries?: number,
  ): Promise<{ serializedInputs: string; status: string }> {
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
        executor_id,
        application_version,
        application_id,
        created_at,
        recovery_attempts,
        updated_at
      ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
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
        JSON.stringify(initStatus.authenticatedRoles),
        JSON.stringify(initStatus.request),
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
    if (maxRetries && attempts > maxRetries + 1) {
      await this.pool.query(
        `UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status SET status=$1 WHERE workflow_uuid=$2 AND status=$3`,
        [StatusString.RETRIES_EXCEEDED, initStatus.workflowUUID, StatusString.PENDING],
      );
      throw new DBOSDeadLetterQueueError(initStatus.workflowUUID, maxRetries);
    }
    this.logger.debug(`Workflow ${initStatus.workflowUUID} attempt number: ${attempts}.`);
    const status = resRow.status;

    const { rows } = await this.pool.query<workflow_inputs>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_inputs (workflow_uuid, inputs) VALUES($1, $2) ON CONFLICT (workflow_uuid) DO UPDATE SET workflow_uuid = excluded.workflow_uuid  RETURNING inputs`,
      [initStatus.workflowUUID, serializedInputs],
    );
    if (serializedInputs !== rows[0].inputs) {
      this.logger.warn(
        `Workflow inputs for ${initStatus.workflowUUID} changed since the first call! Use the original inputs.`,
      );
    }
    return { serializedInputs: rows[0].inputs, status };
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
      throw new DBOSWorkflowConflictError(`Attempt to record transition of nonexistent workflow ${workflowID}`);
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

  async getWorkflowInputs(workflowID: string): Promise<string | null> {
    const { rows } = await this.pool.query<workflow_inputs>(
      `SELECT inputs FROM ${DBOSExecutor.systemDBSchemaName}.workflow_inputs WHERE workflow_uuid=$1`,
      [workflowID],
    );
    if (rows.length === 0) {
      return null;
    }
    return rows[0].inputs;
  }

  async getOperationResult(
    workflowID: string,
    functionID: number,
    client?: PoolClient,
  ): Promise<{ res?: SystemDatabaseStoredResult }> {
    await this.checkIfCanceled(workflowID);
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

  async getOperationResultAndThrowIfCancelled(
    workflowID: string,
    functionID: number,
    client?: PoolClient,
  ): Promise<{ res?: SystemDatabaseStoredResult }> {
    await this.checkIfCanceled(workflowID);
    return await this.getOperationResult(workflowID, functionID, client);
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
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw err;
      }
    }
  }

  async getMaxFunctionID(workflowID: string): Promise<number> {
    const { rows } = await this.pool.query<{ max_function_id: number }>(
      `SELECT max(function_id) as max_function_id FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1`,
      [workflowID],
    );

    return rows.length === 0 ? 0 : rows[0].max_function_id;
  }

  async forkWorkflow(originalWorkflowID: string, forkedWorkflowId: string, startStep: number = 0): Promise<string> {
    const workflowStatus = await this.getWorkflowStatus(originalWorkflowID);

    if (workflowStatus === null) {
      throw new DBOSNonExistentWorkflowError(`Workflow ${originalWorkflowID} does not exist`);
    }

    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const query = `
        INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status (
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
        `;

      await client.query(query, [
        forkedWorkflowId,
        StatusString.ENQUEUED,
        workflowStatus.workflowName,
        workflowStatus.workflowClassName,
        workflowStatus.workflowConfigName,
        INTERNAL_QUEUE_NAME,
        workflowStatus.authenticatedUser,
        workflowStatus.assumedRole,
        JSON.stringify(workflowStatus.authenticatedRoles),
        JSON.stringify(workflowStatus.request),
        null,
        null,
        workflowStatus.applicationVersion,
        workflowStatus.applicationID,
        Date.now(),
        0,
        Date.now(),
      ]);

      // Copy the inputs to the new workflow
      const inputQuery = `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_inputs (workflow_uuid, inputs)
                            SELECT $2, inputs
                            FROM ${DBOSExecutor.systemDBSchemaName}.workflow_inputs
                            WHERE workflow_uuid = $1;`;

      await client.query<workflow_inputs>(inputQuery, [originalWorkflowID, forkedWorkflowId]);

      if (startStep > 0) {
        const query = `
          INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs (
          workflow_uuid,
          function_id,
          output,
          error,
          function_name,
          child_workflow_id
        )
        SELECT
          $1 AS workflow_uuid,
          function_id,
          output,
          error,
          function_name,
          child_workflow_id
          FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
          WHERE workflow_uuid = $2 AND function_id < $3
        `;

        await client.query(query, [forkedWorkflowId, originalWorkflowID, startStep]);
      }

      await client.query<workflow_queue>(
        `
      INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_queue (workflow_uuid, queue_name)
      VALUES ($1, $2)
      ON CONFLICT (workflow_uuid)
      DO NOTHING;
      `,
        [forkedWorkflowId, INTERNAL_QUEUE_NAME],
      );

      await client.query('COMMIT');
      return forkedWorkflowId;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
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
      const res = await this.getOperationResultAndThrowIfCancelled(workflowID, functionID, client);
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

  async durableSleepms(workflowID: string, functionID: number, durationMS: number): Promise<void> {
    let resolveNotification: () => void;
    const cancelPromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });

    const cbr = this.cancelWakeupMap.registerCallback(workflowID, resolveNotification!);
    try {
      let timeoutPromise: Promise<void> = Promise.resolve();
      const { promise, cancel: timeoutCancel } = await this.durableSleepmsInternal(workflowID, functionID, durationMS);
      timeoutPromise = promise;

      try {
        await Promise.race([cancelPromise, timeoutPromise]);
      } finally {
        timeoutCancel();
      }
    } finally {
      this.cancelWakeupMap.deregisterCallback(cbr);
    }

    await this.checkIfCanceled(workflowID);
  }

  async durableSleepmsInternal(
    workflowID: string,
    functionID: number,
    durationMS: number,
    maxSleepPerIteration?: number,
  ): Promise<{ promise: Promise<void>; cancel: () => void; endTime: number }> {
    if (maxSleepPerIteration === undefined) maxSleepPerIteration = durationMS;

    const curTime = Date.now();
    let endTimeMs = curTime + durationMS;
    const res = await this.getOperationResultAndThrowIfCancelled(workflowID, functionID);
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
    return {
      ...cancellableSleep(Math.max(Math.min(maxSleepPerIteration, endTimeMs - curTime), 0)),
      endTime: endTimeMs,
    };
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
    const res = await this.getOperationResultAndThrowIfCancelled(workflowID, functionID);
    if (res.res) {
      if (res.res.functionName !== DBOS_FUNCNAME_RECV) {
        throw new DBOSUnexpectedStepError(workflowID, functionID, DBOS_FUNCNAME_RECV, res.res.functionName!);
      }
      return res.res.res!;
    }

    const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
    let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;

    while (true) {
      // register the key with the global notifications listener.
      let resolveNotification: () => void;
      const messagePromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const payload = `${workflowID}::${topic}`;
      const cbr = this.notificationsMap.registerCallback(payload, resolveNotification!);
      const crh = this.cancelWakeupMap.registerCallback(workflowID, (_res) => {
        resolveNotification();
      });

      try {
        await this.checkIfCanceled(workflowID);

        // Check if the key is already in the DB, then wait for the notification if it isn't.
        const initRecvRows = (
          await this.pool.query<notifications>(
            `SELECT topic FROM ${DBOSExecutor.systemDBSchemaName}.notifications WHERE destination_uuid=$1 AND topic=$2;`,
            [workflowID, topic],
          )
        ).rows;

        if (initRecvRows.length !== 0) break;

        const ct = Date.now();
        if (finishTime && ct > finishTime) break; // Time's up

        let timeoutPromise: Promise<void> = Promise.resolve();
        let timeoutCancel: () => void = () => {};
        if (timeoutms) {
          const { promise, cancel, endTime } = await this.durableSleepmsInternal(
            workflowID,
            timeoutFunctionID,
            timeoutms,
            this.dbPollingIntervalEventMs,
          );
          timeoutPromise = promise;
          timeoutCancel = cancel;
          finishTime = endTime;
        } else {
          let poll = finishTime ? finishTime - ct : this.dbPollingIntervalEventMs;
          poll = Math.min(this.dbPollingIntervalEventMs, poll);
          const { promise, cancel } = cancellableSleep(poll);
          timeoutPromise = promise;
          timeoutCancel = cancel;
        }
        try {
          await Promise.race([messagePromise, timeoutPromise]);
        } finally {
          timeoutCancel();
        }
      } finally {
        this.notificationsMap.deregisterCallback(cbr);
        this.cancelWakeupMap.deregisterCallback(crh);
      }
    }

    await this.checkIfCanceled(workflowID);

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
      const res = await this.getOperationResultAndThrowIfCancelled(
        callerWorkflow.workflowID,
        callerWorkflow.functionID,
      );
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
    const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
    let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;

    // Register the key with the global notifications listener first... we do not want to look in the DB first
    //  or that would cause a timing hole.
    while (true) {
      let resolveNotification: () => void;
      const valuePromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const cbr = this.workflowEventsMap.registerCallback(payloadKey, resolveNotification!);
      const crh = callerWorkflow?.workflowID
        ? this.cancelWakeupMap.registerCallback(callerWorkflow.workflowID, (_res) => {
            resolveNotification();
          })
        : undefined;

      try {
        if (callerWorkflow?.workflowID) await this.checkIfCanceled(callerWorkflow?.workflowID);
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
          break;
        }

        const ct = Date.now();
        if (finishTime && ct > finishTime) break; // Time's up

        // If we have a callerWorkflow, we want a durable sleep, otherwise, not
        let timeoutPromise: Promise<void> = Promise.resolve();
        let timeoutCancel: () => void = () => {};
        if (callerWorkflow && timeoutms) {
          const { promise, cancel, endTime } = await this.durableSleepmsInternal(
            callerWorkflow.workflowID,
            callerWorkflow.timeoutFunctionID ?? -1,
            timeoutms,
            this.dbPollingIntervalEventMs,
          );
          timeoutPromise = promise;
          timeoutCancel = cancel;
          finishTime = endTime;
        } else {
          let poll = finishTime ? finishTime - ct : this.dbPollingIntervalEventMs;
          poll = Math.min(this.dbPollingIntervalEventMs, poll);
          const { promise, cancel } = cancellableSleep(poll);
          timeoutPromise = promise;
          timeoutCancel = cancel;
        }

        try {
          await Promise.race([valuePromise, timeoutPromise]);
        } finally {
          timeoutCancel();
        }
      } finally {
        this.workflowEventsMap.deregisterCallback(cbr);
        if (crh) this.cancelWakeupMap.deregisterCallback(crh);
      }
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

  setWFCancelMap(workflowID: string) {
    if (this.runningWorkflowMap.has(workflowID)) {
      this.workflowCancellationMap.set(workflowID, true);
    }
    this.cancelWakeupMap.callCallbacks(workflowID);
  }

  clearWFCancelMap(workflowID: string) {
    if (this.workflowCancellationMap.has(workflowID)) {
      this.workflowCancellationMap.delete(workflowID);
    }
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
      this.logger.error(error);
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    this.setWFCancelMap(workflowID);
  }

  async checkIfCanceled(workflowID: string): Promise<void> {
    if (this.workflowCancellationMap.get(workflowID) === true) {
      throw new DBOSWorkflowCancelledError(workflowID);
    }
    const { rows } = await this.pool.query<workflow_status>(
      `SELECT status FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    if (rows[0].status === StatusString.CANCELLED) {
      throw new DBOSWorkflowCancelledError(workflowID);
    }
    return Promise.resolve();
  }

  async resumeWorkflow(workflowID: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');

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

      await client.query(
        `UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status 
         SET status = $1, queue_name= $2, recovery_attempts = 0 
         WHERE workflow_uuid = $3`,
        [StatusString.ENQUEUED, INTERNAL_QUEUE_NAME, workflowID],
      );

      await client.query<workflow_queue>(
        `
        INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_queue (workflow_uuid, queue_name)
        VALUES ($1, $2)
        ON CONFLICT (workflow_uuid)
        DO NOTHING;
      `,
        [workflowID, INTERNAL_QUEUE_NAME],
      );

      await client.query('COMMIT');
    } catch (error) {
      this.logger.error(error);
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    this.clearWFCancelMap(workflowID);
  }

  registerRunningWorkflow(workflowID: string, workflowPromise: Promise<unknown>) {
    // Need to await for the workflow and capture errors.
    const awaitWorkflowPromise = workflowPromise
      .catch((error) => {
        this.logger.debug('Captured error in awaitWorkflowPromise: ' + error);
      })
      .finally(() => {
        // Remove itself from pending workflow map.
        this.runningWorkflowMap.delete(workflowID);
        this.workflowCancellationMap.delete(workflowID);
      });
    this.runningWorkflowMap.set(workflowID, awaitWorkflowPromise);
  }

  async awaitRunningWorkflows(): Promise<void> {
    if (this.runningWorkflowMap.size > 0) {
      this.logger.info('Waiting for pending workflows to finish.');
      await Promise.allSettled(this.runningWorkflowMap.values());
    }
    if (this.workflowEventsMap.map.size > 0) {
      this.logger.warn('Workflow events map is not empty - shutdown is not clean.');
      //throw new Error('Workflow events map is not empty - shutdown is not clean.');
    }
    if (this.notificationsMap.map.size > 0) {
      this.logger.warn('Message notification map is not empty - shutdown is not clean.');
      //throw new Error('Message notification map is not empty - shutdown is not clean.');
    }
  }

  async getWorkflowStatus(
    workflowID: string,
    callerID?: string,
    callerFN?: number,
  ): Promise<WorkflowStatusInternal | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    const sv = await this.runAsStep(
      async () => {
        const statuses = await this.listWorkflows({ workflowIDs: [workflowID] });
        const status = statuses.find((s) => s.workflowUUID === workflowID);
        return status ? JSON.stringify(status) : null;
      },
      DBOS_FUNCNAME_GETSTATUS,
      callerID,
      callerFN,
    );

    return sv ? (JSON.parse(sv) as WorkflowStatusInternal) : null;
  }

  async awaitWorkflowResult(
    workflowID: string,
    timeoutSeconds?: number,
    callerID?: string,
    timerFuncID?: number,
  ): Promise<SystemDatabaseStoredResult | undefined> {
    const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
    let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;

    while (true) {
      let resolveNotification: () => void;
      const statusPromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const irh = this.cancelWakeupMap.registerCallback(workflowID, (_res) => {
        resolveNotification();
      });
      const crh = callerID
        ? this.cancelWakeupMap.registerCallback(callerID, (_res) => {
            resolveNotification();
          })
        : undefined;
      try {
        if (callerID) await this.checkIfCanceled(callerID);
        try {
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
            } else if (status === StatusString.CANCELLED) {
              return { cancelled: true };
            } else {
              // Status is not actionable
            }
          }
        } catch (e) {
          const err = e as Error;
          this.logger.error(`Exception from system database: ${err}`);
          throw err;
        }

        const ct = Date.now();
        if (finishTime && ct > finishTime) return undefined; // Time's up

        let timeoutPromise: Promise<void> = Promise.resolve();
        let timeoutCancel: () => void = () => {};
        if (timerFuncID !== undefined && callerID !== undefined && timeoutms !== undefined) {
          const { promise, cancel, endTime } = await this.durableSleepmsInternal(
            callerID,
            timerFuncID,
            timeoutms,
            this.dbPollingIntervalResultMs,
          );
          finishTime = endTime;
          timeoutPromise = promise;
          timeoutCancel = cancel;
        } else {
          let poll = finishTime ? finishTime - ct : this.dbPollingIntervalResultMs;
          poll = Math.min(this.dbPollingIntervalResultMs, poll);
          const { promise, cancel } = cancellableSleep(poll);
          timeoutPromise = promise;
          timeoutCancel = cancel;
        }

        try {
          await Promise.race([statusPromise, timeoutPromise]);
        } finally {
          timeoutCancel();
        }
      } finally {
        this.cancelWakeupMap.deregisterCallback(irh);
        if (crh) this.cancelWakeupMap.deregisterCallback(crh);
      }
    }
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
      if (!this.shouldUseDBNotifications) return; // Testing parameter
      if (msg.channel === 'dbos_notifications_channel') {
        if (msg.payload) {
          this.notificationsMap.callCallbacks(msg.payload);
        }
      } else if (msg.channel === 'dbos_workflow_events_channel') {
        if (msg.payload) {
          this.workflowEventsMap.callCallbacks(msg.payload);
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

  async listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatusInternal[]> {
    const schemaName = DBOSExecutor.systemDBSchemaName;

    input.sortDesc = input.sortDesc ?? false; // By default, sort in ascending order
    let query = this.knexDB<workflow_status>(`${schemaName}.workflow_status`)
      .join<workflow_inputs>(
        `${schemaName}.workflow_inputs`,
        `${schemaName}.workflow_status.workflow_uuid`,
        `${schemaName}.workflow_inputs.workflow_uuid`,
      )
      .orderBy(`${schemaName}.workflow_status.created_at`, input.sortDesc ? 'desc' : 'asc');
    if (input.workflowName) {
      query = query.where(`${schemaName}.workflow_status.name`, input.workflowName);
    }
    if (input.workflow_id_prefix) {
      query = query.whereLike(`${schemaName}.workflow_status.workflow_uuid`, `${input.workflow_id_prefix}%`);
    }
    if (input.workflowIDs) {
      query = query.whereIn(`${schemaName}.workflow_status.workflow_uuid`, input.workflowIDs);
    }
    if (input.authenticatedUser) {
      query = query.where(`${schemaName}.workflow_status.authenticated_user`, input.authenticatedUser);
    }
    if (input.startTime) {
      query = query.where(`${schemaName}.workflow_status.created_at`, '>=', new Date(input.startTime).getTime());
    }
    if (input.endTime) {
      query = query.where(`${schemaName}.workflow_status.created_at`, '<=', new Date(input.endTime).getTime());
    }
    if (input.status) {
      query = query.where(`${schemaName}.workflow_status.status`, input.status);
    }
    if (input.applicationVersion) {
      query = query.where(`${schemaName}.workflow_status.application_version`, input.applicationVersion);
    }
    if (input.limit) {
      query = query.limit(input.limit);
    }
    if (input.offset) {
      query = query.offset(input.offset);
    }
    const rows = await query;
    return rows.map(PostgresSystemDatabase.#mapWorkflowStatus);
  }

  async listQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<WorkflowStatusInternal[]> {
    const schemaName = DBOSExecutor.systemDBSchemaName;

    const sortDesc = input.sortDesc ?? false; // By default, sort in ascending order
    let query = this.knexDB<workflow_queue>(`${schemaName}.workflow_queue`)
      .join<workflow_inputs>(
        `${schemaName}.workflow_inputs`,
        `${schemaName}.workflow_queue.workflow_uuid`,
        `${schemaName}.workflow_inputs.workflow_uuid`,
      )
      .join<workflow_status>(
        `${schemaName}.workflow_status`,
        `${schemaName}.workflow_queue.workflow_uuid`,
        `${schemaName}.workflow_status.workflow_uuid`,
      )
      .orderBy(`${schemaName}.workflow_status.created_at`, sortDesc ? 'desc' : 'asc');

    if (input.workflowName) {
      query = query.whereRaw(`${schemaName}.workflow_status.name = ?`, [input.workflowName]);
    }
    if (input.queueName) {
      query = query.whereRaw(`${schemaName}.workflow_status.queue_name = ?`, [input.queueName]);
    }
    if (input.startTime) {
      query = query.where(`${schemaName}.workflow_status.created_at`, '>=', new Date(input.startTime).getTime());
    }
    if (input.endTime) {
      query = query.where(`${schemaName}.workflow_status.created_at`, '<=', new Date(input.endTime).getTime());
    }
    if (input.status) {
      query = query.whereRaw(`${schemaName}.workflow_status.status = ?`, [input.status]);
    }
    if (input.limit) {
      query = query.limit(input.limit);
    }
    if (input.offset) {
      query = query.offset(input.offset);
    }

    const rows = await query;
    return rows.map(PostgresSystemDatabase.#mapWorkflowStatus);
  }

  static #mapWorkflowStatus(row: workflow_status & workflow_inputs): WorkflowStatusInternal {
    return {
      workflowUUID: row.workflow_uuid,
      status: row.status,
      workflowName: row.name,
      output: row.output ? row.output : null,
      error: row.error ? row.error : null,
      workflowClassName: row.class_name ?? '',
      workflowConfigName: row.config_name ?? '',
      queueName: row.queue_name,
      authenticatedUser: row.authenticated_user,
      assumedRole: row.assumed_role,
      authenticatedRoles: JSON.parse(row.authenticated_roles) as string[],
      request: JSON.parse(row.request) as HTTPRequest,
      executorId: row.executor_id,
      createdAt: Number(row.created_at),
      updatedAt: Number(row.updated_at),
      applicationVersion: row.application_version,
      applicationID: row.application_id,
      recoveryAttempts: Number(row.recovery_attempts),
      input: row.inputs,
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
      const time = Date.now();
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
    const startTimeMs = Date.now();
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
