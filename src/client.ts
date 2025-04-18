import { PoolConfig } from 'pg';
import { PostgresSystemDatabase, SystemDatabase, WorkflowStatusInternal } from './system_database';
import { GlobalLogger as Logger } from './telemetry/logs';
import { v4 as uuidv4 } from 'uuid';
import { RetrievedHandle, StatusString, WorkflowHandle } from './workflow';
import { parseDbString, parseSSLConfig } from './dbos-runtime/config';
import { DBOSJSON } from './utils';

/**
 * EnqueueOptions defines the options that can be passed to the `enqueue` method of the DBOSClient.
 * This includes parameters like queue name, workflow name, workflow class name, and other optional settings.
 */
interface EnqueueOptions {
  /**
   * The name of the queue to which the workflow will be enqueued.
   */
  queueName: string;
  /**
   * The name of the method that will be invoked when the workflow runs.
   */
  workflowName: string;
  /**
   * The name of the class containing the method that will be invoked when the workflow runs.
   */
  workflowClassName: string;
  /**
   * An optional identifier for the workflow to ensure idempotency.
   * If not provided, a new UUID will be generated.
   */
  workflowID?: string;
  /**
   * The application version associated with this workflow.
   * If not provided, the version of the DBOS app that first dequeues the workflow will be used.
   */
  appVersion?: string;
}

/**
 * DBOSClient is the main entry point for interacting with the DBOS system.
 */
export class DBOSClient {
  private readonly logger: Logger;
  private readonly systemDatabase: SystemDatabase;

  private constructor(databaseUrl: string, systemDatabase?: string) {
    const dbConfig = parseDbString(databaseUrl);

    const poolConfig: PoolConfig = {
      host: dbConfig.hostname,
      port: dbConfig.port,
      user: dbConfig.username,
      password: dbConfig.password,
      database: dbConfig.app_db_name,
      ssl: parseSSLConfig(dbConfig),
      connectionTimeoutMillis: dbConfig.connectionTimeoutMillis,
    };

    systemDatabase ??= `${dbConfig.app_db_name}_dbos_sys`;

    this.logger = new Logger();
    this.systemDatabase = new PostgresSystemDatabase(poolConfig, systemDatabase, this.logger);
  }

  /**
   * Creates a new instance of the DBOSClient.
   * @param databaseUrl - The connection string for the database. This should include the hostname, port, username, password, and database name.
   * @param systemDatabase - An optional name for the system database. If not provided, it defaults to the application database name with a `_dbos_sys` suffix.
   * @returns A Promise that resolves with the DBOSClient instance.
   */
  static async create(databaseUrl: string, systemDatabase?: string): Promise<DBOSClient> {
    const client = new DBOSClient(databaseUrl, systemDatabase);
    await client.systemDatabase.init();
    return client;
  }

  /**
   * Destroys the underlying database connection.
   * This should be called when the client is no longer needed to clean up resources.
   * @returns A Promise that resolves when database connection is destroyed.
   */
  async destroy() {
    await this.systemDatabase.destroy();
  }

  /**
   * Enqueues a workflow for execution.
   * @param options - Options for the enqueue operation, including queue name, workflow name, and other parameters.
   * @param args - Arguments to pass to the workflow upon execution.
   * @returns A Promise that resolves when the message has been sent.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async enqueue<T extends (...args: any[]) => Promise<any>>(
    options: EnqueueOptions,
    ...args: Parameters<T>
  ): Promise<WorkflowHandle<Awaited<ReturnType<T>>>> {
    const { workflowName, workflowClassName, queueName, appVersion } = options;
    const workflowUUID = options.workflowID ?? uuidv4();

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowUUID,
      status: StatusString.ENQUEUED,
      workflowName: workflowName,
      workflowClassName: workflowClassName,
      workflowConfigName: '',
      queueName: queueName,
      authenticatedUser: '',
      output: null,
      error: null,
      assumedRole: '',
      authenticatedRoles: [],
      request: {},
      executorId: '',
      applicationVersion: appVersion,
      applicationID: '',
      createdAt: Date.now(),
      maxRetries: 50,
    };

    await this.systemDatabase.initWorkflowStatus(internalStatus, args);
    await this.systemDatabase.enqueueWorkflow(workflowUUID, queueName);
    return new RetrievedHandle<Awaited<ReturnType<T>>>(this.systemDatabase, workflowUUID);
  }

  /**
   * Sends a message to a workflow, identified by destinationID.
   * @param destinationID - The ID of the destination workflow.
   * @param message - The message to send. This can be any serializable object.
   * @param topic - An optional topic to send the message to. If not provided, the default topic will be used.
   * @param idempotencyKey - An optional idempotency key to ensure that the message is only sent once.
   * @returns A Promise that resolves when the message has been sent.
   */
  async send<T>(destinationID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    idempotencyKey ??= uuidv4();
    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: `${destinationID}-${idempotencyKey}`,
      status: StatusString.SUCCESS,
      workflowName: 'temp_workflow-send-client',
      workflowClassName: '',
      workflowConfigName: '',
      authenticatedUser: '',
      output: null,
      error: null,
      assumedRole: '',
      authenticatedRoles: [],
      request: {},
      executorId: '',
      applicationID: '',
      createdAt: Date.now(),
      maxRetries: 50,
    };
    await this.systemDatabase.initWorkflowStatus(internalStatus, [destinationID, message, topic]);
    await this.systemDatabase.send(internalStatus.workflowUUID, 0, destinationID, DBOSJSON.stringify(message), topic);
  }

  /**
   * Retrieves an event published by workflowID for a given key.
   * @param workflowID - The ID of the workflow that published the event.
   * @param key - The key associated with the event you want to retrieve.
   * @param timeoutSeconds - Optional timeout in seconds for how long to wait for the event to be available.
   * @returns A Promise that resolves with the event payload.
   */
  async getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null> {
    return DBOSJSON.parse(await this.systemDatabase.getEvent(workflowID, key, timeoutSeconds ?? 60)) as T;
  }

  /**
   * Retrieves a single workflow by its id.
   * @param workflowID - The ID of the workflow to retrieve.
   * @returns a WorkflowHandle that represents the retrieved workflow.
   */
  retrieveWorkflow<T = unknown>(workflowID: string): WorkflowHandle<Awaited<T>> {
    return new RetrievedHandle(this.systemDatabase, workflowID);
  }
}
