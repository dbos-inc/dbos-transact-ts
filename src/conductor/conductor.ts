import { DBOSExecutor } from '../dbos-executor';
import { DBOSJSON, globalParams } from '../utils';
import WebSocket from 'ws';
import {
  BaseMessage,
  CancelRequest,
  CancelResponse,
  ExecutorInfoResponse,
  ExistPendingWorkflowsRequest,
  ExistPendingWorkflowsResponse,
  GetWorkflowRequest,
  GetWorkflowResponse,
  ListQueuedWorkflowsRequest,
  ListQueuedWorkflowsResponse,
  ListWorkflowsRequest,
  ListWorkflowsResponse,
  MessageType,
  RecoveryRequest,
  RecoveryResponse,
  RestartRequest,
  RestartResponse,
  ResumeRequest,
  ResumeResponse,
  WorkflowsOutput,
} from './protocol';
import { GetWorkflowsInput, StatusString } from '..';
import { getWorkflowInfo } from '../dbos-runtime/workflow_management';
import { GetQueuedWorkflowsInput } from '../workflow';

export interface ConductorParams {
  conductorURL?: string;
  conductorKey: string;
}
export class Conductor {
  url: string;
  websocket: WebSocket | undefined = undefined;
  isShuttingDown = false;
  isClosed = false;

  constructor(
    readonly dbosExec: DBOSExecutor,
    readonly params: ConductorParams,
  ) {
    const appName = globalParams.appName;
    const cleanConductorURL = params.conductorURL!.replace(/\/+$/, '');
    this.url = `${cleanConductorURL}/websocket/${appName}/${params.conductorKey}`;
  }

  dispatchLoop() {
    if (this.websocket) {
      this.dbosExec.logger.warn('Conductor websocket already exists');
      return;
    }

    try {
      this.dbosExec.logger.debug(`Connecting to conductor at ${this.url}`);
      // Start a new websocket connection
      this.websocket = new WebSocket(this.url);
      this.websocket.on('open', () => {
        this.dbosExec.logger.debug('Opened connection to DBOS conductor');
      });

      this.websocket.on('message', async (data: string) => {
        this.dbosExec.logger.debug(`Received message from conductor: ${data}`);
        const baseMsg = DBOSJSON.parse(data) as BaseMessage;
        const msgType = baseMsg.type;
        switch (msgType) {
          case MessageType.EXECUTOR_INFO:
            const infoResp = new ExecutorInfoResponse(
              baseMsg.request_id,
              globalParams.executorID,
              globalParams.appVersion,
            );
            this.websocket!.send(DBOSJSON.stringify(infoResp));
            this.dbosExec.logger.info('Connected to DBOS conductor');
            break;
          case MessageType.RECOVERY:
            const recoveryMsg = baseMsg as RecoveryRequest;
            let success = true;
            try {
              await this.dbosExec.recoverPendingWorkflows(recoveryMsg.executor_ids);
            } catch (e) {
              this.dbosExec.logger.error(`Exception encountered when recovering workflows: ${(e as Error).message}`);
              success = false;
            }
            const recoveryResp = new RecoveryResponse(baseMsg.request_id, success);
            this.websocket!.send(DBOSJSON.stringify(recoveryResp));
            break;
          case MessageType.CANCEL:
            const cancelMsg = baseMsg as CancelRequest;
            let cancelSuccess = true;
            try {
              await this.dbosExec.cancelWorkflow(cancelMsg.workflow_id);
            } catch (e) {
              this.dbosExec.logger.error(
                `Exception encountered when cancelling workflow ${cancelMsg.workflow_id}: ${(e as Error).message}`,
              );
              cancelSuccess = false;
            }
            const cancelResp = new CancelResponse(baseMsg.request_id, cancelSuccess);
            this.websocket!.send(DBOSJSON.stringify(cancelResp));
            break;
          case MessageType.RESUME:
            const resumeMsg = baseMsg as ResumeRequest;
            let resumeSuccess = true;
            try {
              await this.dbosExec.resumeWorkflow(resumeMsg.workflow_id);
            } catch (e) {
              this.dbosExec.logger.error(
                `Exception encountered when resuming workflow ${resumeMsg.workflow_id}: ${(e as Error).message}`,
              );
              resumeSuccess = false;
            }
            const resumeResp = new ResumeResponse(baseMsg.request_id, resumeSuccess);
            this.websocket!.send(DBOSJSON.stringify(resumeResp));
            break;
          case MessageType.RESTART:
            const restartMsg = baseMsg as RestartRequest;
            let restartSuccess = true;
            try {
              await this.dbosExec.executeWorkflowUUID(restartMsg.workflow_id, true);
            } catch (e) {
              this.dbosExec.logger.error(
                `Exception encountered when restarting workflow ${restartMsg.workflow_id}: ${(e as Error).message}`,
              );
              restartSuccess = false;
            }
            const restartResp = new RestartResponse(baseMsg.request_id, restartSuccess);
            this.websocket!.send(DBOSJSON.stringify(restartResp));
            break;
          case MessageType.LIST_WORKFLOWS:
            const listWFMsg = baseMsg as ListWorkflowsRequest;
            const body = listWFMsg.body;
            const listWFReq: GetWorkflowsInput = {
              workflowIDs: body.workflow_uuids,
              workflowName: body.workflow_name,
              authenticatedUser: body.authenticated_user,
              startTime: body.start_time,
              endTime: body.end_time,
              status: body.status as (typeof StatusString)[keyof typeof StatusString],
              applicationVersion: body.application_version,
              limit: body.limit,
              offset: body.offset,
              sortDesc: body.sort_desc,
            };
            const wfIDs = (await this.dbosExec.systemDatabase.getWorkflows(listWFReq)).workflowUUIDs;
            const workflowsOutput = await Promise.all(
              wfIDs.map(async (i) => {
                const wfInfo = await getWorkflowInfo(this.dbosExec.systemDatabase, i, false);
                return new WorkflowsOutput(wfInfo);
              }),
            );
            const wfsResp = new ListWorkflowsResponse(listWFMsg.request_id, workflowsOutput);
            this.websocket!.send(DBOSJSON.stringify(wfsResp));
            break;
          case MessageType.LIST_QUEUED_WORKFLOWS:
            const listQueuedWFMsg = baseMsg as ListQueuedWorkflowsRequest;
            const bodyQueued = listQueuedWFMsg.body;
            const listQueuedWFReq: GetQueuedWorkflowsInput = {
              workflowName: bodyQueued.workflow_name,
              startTime: bodyQueued.start_time,
              endTime: bodyQueued.end_time,
              status: bodyQueued.status as (typeof StatusString)[keyof typeof StatusString],
              limit: bodyQueued.limit,
              queueName: bodyQueued.queue_name,
              offset: bodyQueued.offset,
              sortDesc: bodyQueued.sort_desc,
            };
            const queuedWFIDs = (await this.dbosExec.systemDatabase.getQueuedWorkflows(listQueuedWFReq)).workflowUUIDs;
            const queuedWFOutput = await Promise.all(
              queuedWFIDs.map(async (i) => {
                const wfInfo = await getWorkflowInfo(this.dbosExec.systemDatabase, i, false);
                return new WorkflowsOutput(wfInfo);
              }),
            );
            const queuedWfsResp = new ListQueuedWorkflowsResponse(listQueuedWFMsg.request_id, queuedWFOutput);
            this.websocket!.send(DBOSJSON.stringify(queuedWfsResp));
            break;
          case MessageType.GET_WORKFLOW:
            const getWFMsg = baseMsg as GetWorkflowRequest;
            const wfInfo = await getWorkflowInfo(this.dbosExec.systemDatabase, getWFMsg.workflow_id, false);
            const wfOutput = new WorkflowsOutput(wfInfo);
            const getWFResp = new GetWorkflowResponse(getWFMsg.request_id, wfOutput);
            this.websocket!.send(DBOSJSON.stringify(getWFResp));
            break;
          case MessageType.EXIST_PENDING_WORKFLOWS:
            const existPendingMsg = baseMsg as ExistPendingWorkflowsRequest;
            const pendingWFs = await this.dbosExec.systemDatabase.getPendingWorkflows(
              existPendingMsg.executor_id,
              existPendingMsg.application_version,
            );
            const existPendingResp = new ExistPendingWorkflowsResponse(baseMsg.request_id, pendingWFs.length > 0);
            this.websocket!.send(DBOSJSON.stringify(existPendingResp));
            break;
          default:
            this.dbosExec.logger.error(`Unknown message type: ${baseMsg.type}`);
        }
      });

      this.websocket.on('close', () => {
        if (this.isShuttingDown) {
          this.dbosExec.logger.info('Shutdown Conductor connection');
          this.isClosed = true;
          return;
        } else {
          // Try to reconnect
          this.dbosExec.logger.error('Connection to conductor lost. Reconnecting.');
          setTimeout(() => {
            this.websocket?.terminate();
            this.websocket = undefined;
            this.dispatchLoop();
          }, 1000);
        }
      });

      this.websocket.on('error', (err) => {
        console.error(err);
        // TODO: better error message, showing the detailed error.
        this.dbosExec.logger.error(`Unexpected exception in connection to conductor. Reconnecting: ${err.message}`);
        setTimeout(() => {
          this.websocket?.terminate();
          this.websocket = undefined;
          this.dispatchLoop();
        }, 1000);
      });
    } catch (e) {
      this.dbosExec.logger.error(`Error in conductor loop. Reconnecting: ${(e as Error).message}`);
      setTimeout(() => {
        this.websocket?.terminate();
        this.websocket = undefined;
        this.dispatchLoop();
      }, 1000);
    }
  }

  stop() {
    this.isShuttingDown = true;
    if (this.websocket) {
      this.websocket.close();
    }
  }
}
