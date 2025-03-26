import { PoolConfig } from 'pg';
import { PostgresSystemDatabase, SystemDatabase, WorkflowStatusInternal } from './system_database';
import { GlobalLogger as Logger } from './telemetry/logs';
import { v4 as uuidv4 } from 'uuid';
import { RetrievedHandle, StatusString, WorkflowHandle } from './workflow';

interface EnqueueOptions {
  queueName: string;
  workflowName: string;
  workflowClassName: string;
  workflowID?: string;
  maxRetries?: number;
  appVersion?: string;
}

export class DBOSClient {
  private readonly logger: Logger;
  private readonly systemDatabase: SystemDatabase;

  constructor(poolConfig: PoolConfig, systemDatabase: string) {
    this.logger = new Logger();
    this.systemDatabase = new PostgresSystemDatabase(poolConfig, systemDatabase, this.logger);
  }

  async init() {
    await this.systemDatabase.init();
  }

  async destroy() {
    await this.systemDatabase.destroy();
  }

  async enqueue<T extends unknown[]>(options: EnqueueOptions, ...args: T): Promise<void> {
    const { workflowName, workflowClassName, queueName, appVersion } = options;
    const workflowUUID = options.workflowID ?? uuidv4();
    const maxRetries = options.maxRetries ?? 50;

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowUUID,
      status: StatusString.ENQUEUED,
      workflowName: workflowName,
      workflowClassName: workflowClassName,
      workflowConfigName: '',
      queueName: queueName,
      authenticatedUser: '',
      output: undefined,
      error: '',
      assumedRole: '',
      authenticatedRoles: [],
      request: {},
      executorId: '',
      applicationVersion: appVersion,
      applicationID: '',
      createdAt: Date.now(),
      maxRetries: maxRetries ?? 50,
    };

    await this.systemDatabase.initWorkflowStatus(internalStatus, args);
    await this.systemDatabase.enqueueWorkflow(workflowUUID, queueName);
  }

  async send<T>(destinationID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    idempotencyKey ??= uuidv4();
    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: `${destinationID}-${idempotencyKey}`,
      status: StatusString.SUCCESS,
      workflowName: 'temp_workflow-send-client',
      workflowClassName: '',
      workflowConfigName: '',
      authenticatedUser: '',
      output: undefined,
      error: '',
      assumedRole: '',
      authenticatedRoles: [],
      request: {},
      executorId: '',
      applicationID: '',
      createdAt: Date.now(),
      maxRetries: 50,
    };
    await this.systemDatabase.initWorkflowStatus(internalStatus, [destinationID, message, topic]);
    await this.systemDatabase.send(internalStatus.workflowUUID, 0, destinationID, message, topic);
  }

  async getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null> {
    return await this.systemDatabase.getEvent(workflowID, key, timeoutSeconds ?? 60);
  }

  retrieveWorkflow<T = unknown>(workflowID: string): WorkflowHandle<Awaited<T>> {
    return new RetrievedHandle(this.systemDatabase, workflowID);
  }
}
