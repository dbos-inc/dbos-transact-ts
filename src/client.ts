import { PoolConfig } from 'pg';
import { PostgresSystemDatabase, SystemDatabase, WorkflowStatusInternal } from './system_database';
import { GlobalLogger as Logger } from './telemetry/logs';
import { v4 as uuidv4 } from 'uuid';
import { StatusString } from './workflow';

interface EnqueueOptions {
  queueName: string;
  workflowName: string;
  workflowClassName: string;
  workflowUUID?: string;
  maxRetries?: number;
  appVersion?: string;
}

export class DBOSClient {
  readonly logger: Logger;

  readonly systemDatabase: SystemDatabase;

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
    const { workflowName, workflowClassName, queueName } = options;
    const workflowUUID = options.workflowUUID ?? uuidv4();
    const maxRetries = options.maxRetries ?? 50;
    const appVersion = options.appVersion ?? '';

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
}
