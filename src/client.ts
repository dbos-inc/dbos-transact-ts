import { PoolConfig } from 'pg';
import { PostgresSystemDatabase, SystemDatabase, WorkflowStatusInternal } from './system_database';
import { GlobalLogger as Logger } from './telemetry/logs';
import { v4 as uuidv4 } from 'uuid';
import { DBOSJSON } from './utils';
import { StatusString } from './workflow';

export class DBOSClient {
  readonly logger: Logger;

  readonly systemDatabase: SystemDatabase;

  private constructor(poolConfig: PoolConfig, systemDatabase: string) {
    this.logger = new Logger();
    this.systemDatabase = new PostgresSystemDatabase(poolConfig, systemDatabase, this.logger);
  }

  async init() {
    await this.systemDatabase.init();
  }

  async destroy() {
    await this.systemDatabase.destroy();
  }

  async enqueue<T extends unknown[]>(queueName: string, ...args: T): Promise<void> {
    const workflowUUID = uuidv4();

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowUUID,
      status: StatusString.PENDING,
      workflowName: '', //wf.name, TODO
      workflowClassName: '', //wCtxt.isTempWorkflow ? '' : (0, decorators_1.getRegisteredMethodClassName)(wf), TODO
      workflowConfigName: '', // PUNT
      queueName: queueName,
      authenticatedUser: '',
      output: undefined,
      error: '',
      assumedRole: '',
      authenticatedRoles: [],
      request: {},
      executorId: '', // NULL - filled in on dequeue
      applicationVersion: '', // NULL, cloud only thing
      applicationID: '', //Not sure how to do this yet
      createdAt: Date.now(), // Remember the start time of this workflow
      maxRetries: 50, //wCtxt.maxRecoveryAttempts,
    };

    const { args: initArgs, status } = await this.systemDatabase.initWorkflowStatus(internalStatus, args);
    await this.systemDatabase.enqueueWorkflow(workflowUUID, queueName);
  }

  async send<T>(destinationID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {}

  async getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null> {
    return null;
  }
}
