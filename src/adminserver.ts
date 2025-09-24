import Koa from 'koa';
import Router from '@koa/router';
import { bodyParser } from '@koa/bodyparser';
import cors from '@koa/cors';
import { GetWorkflowsInput, GetQueuedWorkflowsInput, StatusString } from './workflow';
import { DBOSDataValidationError, DBOSError } from './error';
import { DBOSExecutor } from './dbos-executor';
import { GlobalLogger } from './telemetry/logs';
import * as net from 'net';
import { performance } from 'perf_hooks';
import { DBOSJSON, globalParams } from './utils';
import { QueueParameters, wfQueueRunner } from './wfqueue';
import { serializeError } from 'serialize-error';
import { globalTimeout } from './workflow_management';
import * as protocol from './conductor/protocol';

export type QueueMetadataResponse = QueueParameters & { name: string };

export const WorkflowUUIDHeader = 'dbos-idempotency-key';
export const WorkflowRecoveryUrl = '/dbos-workflow-recovery';
export const HealthUrl = '/dbos-healthz';
export const PerfUrl = '/dbos-perf';
export const DeactivateUrl = '/deactivate';
export const WorkflowQueuesMetadataUrl = '/dbos-workflow-queues-metadata';

export class DBOSAdminServer {
  readonly adminApp: Koa;
  readonly logger: GlobalLogger;
  static nRegisteredEndpoints: number = 0;
  static instance?: DBOSAdminServer = undefined;

  /**
   * Create a Koa app.
   * @param dbosExec User pass in an DBOS workflow executor instance.
   * TODO: maybe call dbosExec.init() somewhere in this class?
   */
  constructor(readonly dbosExec: DBOSExecutor) {
    this.logger = dbosExec.logger;
    this.adminApp = DBOSAdminServer.setupAdminApp(this.dbosExec);
    DBOSAdminServer.instance = this;
  }

  static setupAdminApp(dbosExec: DBOSExecutor): Koa {
    const adminRouter = new Router();
    const adminApp = new Koa();
    adminApp.use(bodyParser());
    adminApp.use(cors());

    // Register HTTP endpoints.
    DBOSAdminServer.registerHealthEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerRecoveryEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerPerfEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerDeactivateEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerCancelWorkflowEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerResumeWorkflowEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerRestartWorkflowEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerQueueMetadataEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerListWorkflowStepsEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerListWorkflowsEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerListQueuedWorkflowsEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerGetWorkflowEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerForkWorkflowEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerGarbageCollectEndpoint(dbosExec, adminRouter);
    DBOSAdminServer.registerGlobalTimeoutEndpoint(dbosExec, adminRouter);
    adminApp.use(adminRouter.routes()).use(adminRouter.allowedMethods());
    return adminApp;
  }

  static async checkPortAvailabilityIPv4Ipv6(port: number, logger: GlobalLogger) {
    try {
      await this.checkPortAvailability(port, '127.0.0.1');
    } catch (error) {
      const err = error as NodeJS.ErrnoException;
      if (err.code === 'EADDRINUSE') {
        logger.warn(
          `Port ${port} is already used for IPv4 address "127.0.0.1". Please use the -p option to choose another port.\n${err.message}`,
        );
        throw error;
      } else {
        logger.warn(
          `Error occurred while checking port availability for IPv4 address "127.0.0.1" : ${err.code}\n${err.message}`,
        );
      }
    }

    try {
      await this.checkPortAvailability(port, '::1');
    } catch (error) {
      const err = error as NodeJS.ErrnoException;
      if (err.code === 'EADDRINUSE') {
        logger.warn(
          `Port ${port} is already used for IPv6 address "::1". Please use the -p option to choose another port.\n${err.message}`,
        );
        throw error;
      } else {
        logger.warn(
          `Error occurred while checking port availability for IPv6 address "::1" : ${err.code}\n${err.message}`,
        );
      }
    }
  }

  static async checkPortAvailability(port: number, host: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const server = new net.Server();
      server.on('error', (error: NodeJS.ErrnoException) => {
        reject(error);
      });

      server.on('listening', () => {
        server.close();
        resolve();
      });

      server.listen({ port: port, host: host }, () => {
        resolve();
      });
    });
  }

  /**
   * Health check endpoint.
   */
  static registerHealthEndpoint(dbosExec: DBOSExecutor, router: Router) {
    // Handler function that parses request for recovery.
    const healthHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
      koaCtxt.body = 'healthy';
      await koaNext();
    };
    router.get(HealthUrl, healthHandler);
    dbosExec.logger.debug(`DBOS Server Registered Healthz GET ${HealthUrl}`);
  }

  /**
   * Register workflow queue metadata endpoint.
   */
  static registerQueueMetadataEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const queueMetadataHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
      const queueDetailsArray: QueueMetadataResponse[] = [];
      wfQueueRunner.wfQueuesByName.forEach((q, qn) => {
        queueDetailsArray.push({
          name: qn,
          concurrency: q.concurrency,
          workerConcurrency: q.workerConcurrency,
          rateLimit: q.rateLimit,
        });
      });
      koaCtxt.body = queueDetailsArray;

      await koaNext();
    };

    router.get(WorkflowQueuesMetadataUrl, queueMetadataHandler);
    dbosExec.logger.debug(`DBOS Server Registered Queue Metadata GET ${WorkflowQueuesMetadataUrl}`);
  }

  /**
   * Register workflow recovery endpoint.
   * Receives a list of executor IDs and returns a list of workflowUUIDs.
   */
  static registerRecoveryEndpoint(dbosExec: DBOSExecutor, router: Router) {
    // Handler function that parses request for recovery.
    const recoveryHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
      const executorIDs = koaCtxt.request.body as string[];
      dbosExec.logger.info('Recovering workflows for executors: ' + executorIDs.toString());
      const recoverHandles = await dbosExec.recoverPendingWorkflows(executorIDs);

      // Return a list of workflowUUIDs being recovered.
      koaCtxt.body = await Promise.allSettled(recoverHandles.map((i) => i.workflowID)).then((results) =>
        results.filter((i) => i.status === 'fulfilled').map((i) => (i as PromiseFulfilledResult<unknown>).value),
      );
      await koaNext();
    };

    router.post(WorkflowRecoveryUrl, recoveryHandler);
    dbosExec.logger.debug(`DBOS Server Registered Recovery POST ${WorkflowRecoveryUrl}`);
  }

  /**
   * Register performance endpoint.
   * Returns information on VM performance since last call.
   */
  static registerPerfEndpoint(dbosExec: DBOSExecutor, router: Router) {
    let lastELU = performance.eventLoopUtilization();
    const perfHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
      const currELU = performance.eventLoopUtilization();
      const elu = performance.eventLoopUtilization(currELU, lastELU);
      koaCtxt.body = elu;
      lastELU = currELU;
      await koaNext();
    };
    router.get(PerfUrl, perfHandler);
    dbosExec.logger.debug(`DBOS Server Registered Perf GET ${PerfUrl}`);
  }

  /**
   * Register Deactivate endpoint.
   * Deactivate consumers so that they don't start new workflows.
   *
   */
  static isDeactivated = false;
  static registerDeactivateEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const deactivateHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
      if (!DBOSAdminServer.isDeactivated) {
        dbosExec.logger.info(
          `Deactivating DBOS executor ${globalParams.executorID} with version ${globalParams.appVersion}. This executor will complete existing workflows but will not create new workflows.`,
        );
        DBOSAdminServer.isDeactivated = true;
      }
      await dbosExec.deactivateEventReceivers(false);
      koaCtxt.body = 'Deactivated';
      await koaNext();
    };
    router.get(DeactivateUrl, deactivateHandler);
    dbosExec.logger.debug(`DBOS Server Registered Deactivate GET ${DeactivateUrl}`);
  }

  static registerGarbageCollectEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const url = '/dbos-garbage-collect';
    const handler = async (koaCtxt: Koa.Context) => {
      const body = koaCtxt.request.body as {
        cutoff_epoch_timestamp_ms?: number;
        rows_threshold?: number;
      };
      await dbosExec.systemDatabase.garbageCollect(body.cutoff_epoch_timestamp_ms, body.rows_threshold);
      koaCtxt.status = 204;
    };
    router.post(url, handler);
  }

  static registerGlobalTimeoutEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const url = '/dbos-global-timeout';
    const handler = async (koaCtxt: Koa.Context) => {
      const body = koaCtxt.request.body as {
        cutoff_epoch_timestamp_ms: number;
      };
      await globalTimeout(dbosExec.systemDatabase, body.cutoff_epoch_timestamp_ms);
      koaCtxt.status = 204;
    };
    router.post(url, handler);
  }

  /**
   *
   * Register Cancel Workflow endpoint.
   * Cancels a workflow by setting its status to CANCELLED.
   */

  static registerCancelWorkflowEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const workflowCancelUrl = '/workflows/:workflow_id/cancel';
    const workflowCancelHandler = async (koaCtxt: Koa.Context) => {
      const workflowId = (koaCtxt.params as { workflow_id: string }).workflow_id;
      await dbosExec.cancelWorkflow(workflowId);
      koaCtxt.status = 204;
    };
    router.post(workflowCancelUrl, workflowCancelHandler);
    dbosExec.logger.debug(`DBOS Server Registered Cancel Workflow POST ${workflowCancelUrl}`);
  }

  /**
   *
   * Register Resume Workflow endpoint.
   * Resume a workflow.
   */

  static registerResumeWorkflowEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const workflowResumeUrl = '/workflows/:workflow_id/resume';
    const workflowResumeHandler = async (koaCtxt: Koa.Context) => {
      const workflowId = (koaCtxt.params as { workflow_id: string }).workflow_id;
      dbosExec.logger.info(`Resuming workflow with ID: ${workflowId}`);
      try {
        await dbosExec.resumeWorkflow(workflowId);
      } catch (e) {
        let errorMessage = '';
        if (e instanceof DBOSError) {
          errorMessage = e.message;
        } else {
          errorMessage = `Unknown error`;
        }
        dbosExec.logger.error(`Error resuming workflow ${workflowId}: ${errorMessage}`);
        koaCtxt.status = 500;
        koaCtxt.body = {
          error: `Error resuming workflow ${workflowId}: ${errorMessage}`,
        };
        return;
      }
      koaCtxt.status = 204;
    };
    router.post(workflowResumeUrl, workflowResumeHandler);
    dbosExec.logger.debug(`DBOS Server Registered Cancel Workflow POST ${workflowResumeUrl}`);
  }

  /**
   *
   * Register Restart Workflow endpoint.
   * Restart a workflow.
   */

  static registerRestartWorkflowEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const workflowResumeUrl = '/workflows/:workflow_id/restart';
    const workflowRestartHandler = async (koaCtxt: Koa.Context) => {
      const workflowId = (koaCtxt.params as { workflow_id: string }).workflow_id;
      dbosExec.logger.info(`Restarting workflow: ${workflowId} with a new id`);
      const workflowID = await dbosExec.forkWorkflow(workflowId, 0);
      koaCtxt.body = {
        workflow_id: workflowID,
      };
      koaCtxt.status = 200;
    };
    router.post(workflowResumeUrl, workflowRestartHandler);
    dbosExec.logger.debug(`DBOS Server Registered Cancel Workflow POST ${workflowResumeUrl}`);
  }

  /**
   *
   * Register Fork Workflow endpoint.
   *
   */

  static registerForkWorkflowEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const workflowResumeUrl = '/workflows/:workflow_id/fork';
    const workflowForkHandler = async (koaCtxt: Koa.Context) => {
      const workflowId = (koaCtxt.params as { workflow_id: string }).workflow_id;
      const body = koaCtxt.request.body as {
        start_step?: number;
        new_workflow_id?: string;
        application_version?: string;
        timeout_ms?: number;
      };
      if (body.start_step === undefined) {
        throw new DBOSDataValidationError('Missing start_step in request body');
      }

      dbosExec.logger.info(`Forking workflow: ${workflowId} from step ${body.start_step} with a new id`);
      try {
        const workflowID = await dbosExec.forkWorkflow(workflowId, body.start_step, {
          newWorkflowID: body.new_workflow_id,
          applicationVersion: body.application_version,
          timeoutMS: body.timeout_ms,
        });
        koaCtxt.body = {
          workflow_id: workflowID,
        };
      } catch (e) {
        let errorMessage = '';
        if (e instanceof DBOSError) {
          errorMessage = e.message;
        } else {
          errorMessage = `Unknown error`;
        }
        dbosExec.logger.error(`Error forking workflow ${workflowId}: ${errorMessage}`);
        koaCtxt.status = 500;
        koaCtxt.body = {
          error: `Error forking workflow ${workflowId}: ${errorMessage}`,
        };
        return;
      }
      dbosExec.logger.info(`Forked workflow: ${workflowId} with a new id`);
      koaCtxt.status = 200;
    };
    router.post(workflowResumeUrl, workflowForkHandler);
    dbosExec.logger.debug(`DBOS Server Registered Cancel Workflow POST ${workflowResumeUrl}`);
  }

  /**
   *
   * Register List Workflow Steps endpoint.
   * List steps for a given workflow.
   */

  static registerListWorkflowStepsEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const workflowStepsUrl = '/workflows/:workflow_id/steps';
    const workflowStepsHandler = async (koaCtxt: Koa.Context) => {
      const workflowId = (koaCtxt.params as { workflow_id: string }).workflow_id;
      const steps = await dbosExec.listWorkflowSteps(workflowId);
      koaCtxt.body = steps?.map((step) => ({
        function_name: step.name,
        function_id: step.functionID,
        output: step.output ? DBOSJSON.stringify(step.output) : undefined,
        error: step.error ? DBOSJSON.stringify(serializeError(step.error)) : undefined,
        child_workflow_id: step.childWorkflowID,
      }));
      koaCtxt.status = 200;
    };
    router.get(workflowStepsUrl, workflowStepsHandler);
    dbosExec.logger.debug(`DBOS Server Registered List Workflow steps Get ${workflowStepsUrl}`);
  }

  /**
   *
   * Register List Workflows endpoint.
   * List workflows with optional filtering via request body.
   */
  static registerListWorkflowsEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const listWorkflowsUrl = '/workflows';
    const listWorkflowsHandler = async (koaCtxt: Koa.Context) => {
      const body = koaCtxt.request.body as {
        workflow_uuids?: string[];
        workflow_name?: string;
        authenticated_user?: string;
        start_time?: string;
        end_time?: string;
        status?: (typeof StatusString)[keyof typeof StatusString]; // TODO: this should be a list of statuses.
        application_version?: string;
        limit?: number;
        offset?: number;
        sort_desc?: boolean;
        workflow_id_prefix?: string;
        load_input?: boolean; // Load the input of the workflow (default false)
        load_output?: boolean; // Load the output of the workflow (default false)
      };

      // Map request body keys to GetWorkflowsInput properties
      const input: GetWorkflowsInput = {
        workflowIDs: body.workflow_uuids,
        workflowName: body.workflow_name,
        authenticatedUser: body.authenticated_user,
        startTime: body.start_time,
        endTime: body.end_time,
        status: body.status,
        applicationVersion: body.application_version,
        limit: body.limit,
        offset: body.offset,
        sortDesc: body.sort_desc,
        workflow_id_prefix: body.workflow_id_prefix,
        loadInput: body.load_input ?? false, // Default to false
        loadOutput: body.load_output ?? false, // Default to false
      };

      const workflows = await dbosExec.listWorkflows(input);

      // Map result to the underscore format.
      koaCtxt.body = workflows.map((wf) => new protocol.WorkflowsOutput(wf));
      koaCtxt.status = 200;
    };
    router.post(listWorkflowsUrl, listWorkflowsHandler);
    dbosExec.logger.debug(`DBOS Server Registered List Workflows POST ${listWorkflowsUrl}`);
  }

  /**
   *
   * Register List Queued Workflows endpoint.
   * List queued workflows with optional filtering via request body.
   */
  static registerListQueuedWorkflowsEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const listQueuedWorkflowsUrl = '/queues';
    const listQueuedWorkflowsHandler = async (koaCtxt: Koa.Context) => {
      const body = koaCtxt.request.body as {
        workflow_name?: string;
        start_time?: string;
        end_time?: string;
        status?: (typeof StatusString)[keyof typeof StatusString]; // TODO: this should be a list of statuses.
        queue_name?: string;
        limit?: number;
        offset?: number;
        sort_desc?: boolean;
        load_input?: boolean; // Load the input of the workflow (default false)
      };

      // Map request body keys to GetQueuedWorkflowsInput properties
      const input: GetQueuedWorkflowsInput = {
        workflowName: body.workflow_name,
        startTime: body.start_time,
        endTime: body.end_time,
        status: body.status,
        queueName: body.queue_name,
        limit: body.limit,
        offset: body.offset,
        sortDesc: body.sort_desc,
        loadInput: body.load_input ?? false, // Default to false
      };

      const workflows = await dbosExec.listQueuedWorkflows(input);

      // Map result to the underscore format.
      koaCtxt.body = workflows.map((wf) => new protocol.WorkflowsOutput(wf));
      koaCtxt.status = 200;
    };
    router.post(listQueuedWorkflowsUrl, listQueuedWorkflowsHandler);
    dbosExec.logger.debug(`DBOS Server Registered List Queued Workflows POST ${listQueuedWorkflowsUrl}`);
  }

  /**
   *
   * Register Get Workflow endpoint.
   * Get detailed information about a specific workflow by ID.
   */
  static registerGetWorkflowEndpoint(dbosExec: DBOSExecutor, router: Router) {
    const getWorkflowUrl = '/workflows/:workflow_id';
    const getWorkflowHandler = async (koaCtxt: Koa.Context) => {
      const workflowId = (koaCtxt.params as { workflow_id: string }).workflow_id;
      const workflow = await dbosExec.getWorkflowStatus(workflowId);
      if (workflow) {
        koaCtxt.body = new protocol.WorkflowsOutput(workflow);
        koaCtxt.status = 200;
      } else {
        koaCtxt.status = 404;
        koaCtxt.body = { error: `Workflow ${workflowId} not found` };
      }
    };
    router.get(getWorkflowUrl, getWorkflowHandler);
    dbosExec.logger.debug(`DBOS Server Registered Get Workflow GET ${getWorkflowUrl}`);
  }
}
