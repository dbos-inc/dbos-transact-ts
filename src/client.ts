import {
  PostgresSystemDatabase,
  type SystemDatabase,
  type WorkflowStatusInternal,
  DBOS_STREAM_CLOSED_SENTINEL,
} from './system_database';

import { GlobalLogger } from './telemetry/logs';
import { randomUUID } from 'node:crypto';
import {
  type GetQueuedWorkflowsInput,
  type GetWorkflowsInput,
  isWorkflowActive,
  StatusString,
  type StepInfo,
  type WorkflowHandle,
  type WorkflowStatus,
} from './workflow';
import { DBOSJSON, getClientConfig, sleepms } from './utils';
import {
  forkWorkflow,
  getWorkflow,
  listQueuedWorkflows,
  listWorkflows,
  listWorkflowSteps,
  toWorkflowStatus,
} from './dbos-runtime/workflow_management';
import { PGNodeUserDatabase, type UserDatabase } from './user_database';
import { getSysDatabaseUrlFromUserDb } from './dbos-runtime/config';
import assert from 'node:assert';
import { DBOSExecutor } from './dbos-executor';
import { DBOSAwaitedWorkflowCancelledError } from './error';

/**
 * EnqueueOptions defines the options that can be passed to the `enqueue` method of the DBOSClient.
 * This includes parameters like queue name, workflow name, workflow class name, and other optional settings.
 */
interface ClientEnqueueOptions {
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
   * If not provided, an empty string will be used as the class name.
   */
  workflowClassName?: string;
  /**
   * The name of the ConfiguredInstance containing the method that will be invoked when the workflow runs.
   * If not provided, an empty string will be used as the configured instance name.
   */
  workflowConfigName?: string;
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
  /**
   * Timeout for the workflow execution in milliseconds.
   * Note, timeout starts when the workflow is dequeued.
   * If not provided, the workflow timeout will not be set and the workflow will run to completion.
   */
  workflowTimeoutMS?: number;
  /**
   * An ID used to identify enqueues workflows that will be used for de-duplication.
   * If not provided, no de-duplication will be performed.
   */
  deduplicationID?: string;

  /**
   * An optional priority for the workflow.
   * Workflows with higher priority will be dequeued first.
   */
  priority?: number;
}

export class ClientHandle<R> implements WorkflowHandle<R> {
  constructor(
    readonly systemDatabase: SystemDatabase,
    readonly workflowUUID: string,
  ) {}

  getWorkflowUUID(): string {
    return this.workflowUUID;
  }

  get workflowID(): string {
    return this.workflowUUID;
  }

  async getStatus(): Promise<WorkflowStatus | null> {
    const status = await this.systemDatabase.getWorkflowStatus(this.workflowUUID);
    return status ? toWorkflowStatus(status) : null;
  }

  async getResult(): Promise<R> {
    const res = await this.systemDatabase.awaitWorkflowResult(this.workflowID);
    if (res?.cancelled) {
      throw new DBOSAwaitedWorkflowCancelledError(this.workflowID);
    }
    return DBOSExecutor.reviveResultOrError<R>(res!);
  }

  async getWorkflowInputs<T extends unknown[]>(): Promise<T> {
    const status = (await this.systemDatabase.getWorkflowStatus(this.workflowUUID)) as WorkflowStatusInternal;
    return DBOSJSON.parse(status.input) as T;
  }
}

/**
 * DBOSClient is the main entry point for interacting with the DBOS system.
 */
export class DBOSClient {
  private readonly logger: GlobalLogger;
  private readonly systemDatabase: SystemDatabase;
  private readonly userDatabase: UserDatabase | undefined;

  private constructor(databaseUrl: string | undefined, systemDatabaseUrl: string | undefined) {
    if (!systemDatabaseUrl) {
      assert(databaseUrl, 'At least one of databaseUrl or systemDatabaseUrl must be provided');
      systemDatabaseUrl = getSysDatabaseUrlFromUserDb(databaseUrl);
    }

    this.logger = new GlobalLogger();
    this.systemDatabase = new PostgresSystemDatabase(systemDatabaseUrl, this.logger);

    this.userDatabase = databaseUrl ? new PGNodeUserDatabase(getClientConfig(databaseUrl)) : undefined;
  }

  /**
   * Creates a new instance of the DBOSClient.
   * @param databaseUrl - The connection string for the database. This should include the hostname, port, username, password, and database name.
   * @param systemDatabase - An optional name for the system database. If not provided, it defaults to the application database name with a `_dbos_sys` suffix.
   * @returns A Promise that resolves with the DBOSClient instance.
   */
  static async create({
    databaseUrl,
    systemDatabaseUrl,
  }: {
    databaseUrl?: string;
    systemDatabaseUrl?: string;
  }): Promise<DBOSClient> {
    const client = new DBOSClient(databaseUrl, systemDatabaseUrl);
    return Promise.resolve(client);
  }

  /**
   * Destroys the underlying database connection.
   * This should be called when the client is no longer needed to clean up resources.
   * @returns A Promise that resolves when database connection is destroyed.
   */
  async destroy() {
    await this.systemDatabase.destroy();
    await this.userDatabase?.destroy();
  }

  /**
   * Enqueues a workflow for execution.
   * @param options - Options for the enqueue operation, including queue name, workflow name, and other parameters.
   * @param args - Arguments to pass to the workflow upon execution.
   * @returns A Promise that resolves when the message has been sent.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async enqueue<T extends (...args: any[]) => Promise<any>>(
    options: ClientEnqueueOptions,
    ...args: Parameters<T>
  ): Promise<WorkflowHandle<Awaited<ReturnType<T>>>> {
    const { workflowName, workflowClassName, workflowConfigName, queueName, appVersion } = options;
    const workflowUUID = options.workflowID ?? randomUUID();

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowUUID,
      status: StatusString.ENQUEUED,
      workflowName: workflowName,
      workflowClassName: workflowClassName ?? '',
      workflowConfigName: workflowConfigName ?? '',
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
      timeoutMS: options.workflowTimeoutMS,
      deadlineEpochMS: undefined,
      input: DBOSJSON.stringify(args),
      deduplicationID: options.deduplicationID,
      priority: options.priority ?? 0,
    };

    await this.systemDatabase.initWorkflowStatus(internalStatus);

    return new ClientHandle<Awaited<ReturnType<T>>>(this.systemDatabase, workflowUUID);
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
    idempotencyKey ??= randomUUID();
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
      input: DBOSJSON.stringify([destinationID, message, topic]),
      deduplicationID: undefined,
      priority: 0,
    };
    await this.systemDatabase.initWorkflowStatus(internalStatus);
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
    return new ClientHandle(this.systemDatabase, workflowID);
  }

  cancelWorkflow(workflowID: string): Promise<void> {
    return this.systemDatabase.cancelWorkflow(workflowID);
  }

  resumeWorkflow(workflowID: string): Promise<void> {
    return this.systemDatabase.resumeWorkflow(workflowID);
  }

  forkWorkflow(
    workflowID: string,
    startStep: number,
    options?: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number },
  ): Promise<string> {
    return forkWorkflow(this.systemDatabase, this.userDatabase, workflowID, startStep, options);
  }

  getWorkflow(workflowID: string): Promise<WorkflowStatus | undefined> {
    return getWorkflow(this.systemDatabase, workflowID);
  }

  listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
    return listWorkflows(this.systemDatabase, input);
  }

  listQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<WorkflowStatus[]> {
    return listQueuedWorkflows(this.systemDatabase, input);
  }

  listWorkflowSteps(workflowID: string): Promise<StepInfo[] | undefined> {
    return listWorkflowSteps(this.systemDatabase, this.userDatabase, workflowID);
  }

  /**
   * Read values from a stream as an async generator.
   * This function reads values from a stream identified by the workflowID and key,
   * yielding each value in order until the stream is closed or the workflow terminates.
   * @param workflowID - The ID of the workflow that wrote to the stream
   * @param key - The stream key to read from
   * @returns An async generator that yields each value in the stream until the stream is closed
   */
  async *readStream<T>(workflowID: string, key: string): AsyncGenerator<T, void, unknown> {
    let offset = 0;

    while (true) {
      try {
        const value = await this.systemDatabase.readStream(workflowID, key, offset);
        if (value === DBOS_STREAM_CLOSED_SENTINEL) {
          break;
        }
        yield value as T;
        offset += 1;
      } catch (error: unknown) {
        if (error instanceof Error && error.message.includes('No value found')) {
          // Poll the offset until a value arrives or the workflow terminates
          const status = await this.getWorkflow(workflowID);
          if (!status || !isWorkflowActive(status.status)) {
            break;
          }
          await sleepms(1000); // 1 second polling interval
          continue;
        }
        throw error;
      }
    }
  }
}
