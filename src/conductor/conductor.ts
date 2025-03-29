import { DBOSExecutor } from '../dbos-executor';
import { DBOSJSON, globalParams } from '../utils';
import WebSocket from 'ws';
import * as protocol from './protocol';
import { GetWorkflowsInput, StatusString } from '..';
import { getWorkflowInfo } from '../dbos-runtime/workflow_management';
import { GetQueuedWorkflowsInput } from '../workflow';
import { hostname } from 'node:os';

export class Conductor {
  url: string;
  websocket: WebSocket | undefined = undefined;
  isShuttingDown = false; // Is in the process of shutting down the connection
  isClosed = false; // Is closed after the connection has been terminated
  pingPeriodMs = 20000; // Time in milliseconds to wait before sending a ping message to the conductor
  pingTimeoutMs = 15000; // Time in milliseconds to wait for a response to a ping message before considering the connection dead
  pingInterval: NodeJS.Timeout | undefined = undefined; // Interval for sending ping messages to the conductor
  pingTimeout: NodeJS.Timeout | undefined = undefined; // Timeout for waiting for a response to a ping message
  reconnectDelayMs = 1000;
  reconnectTimeout: NodeJS.Timeout | undefined = undefined;

  constructor(
    readonly dbosExec: DBOSExecutor,
    readonly conductorKey: string,
    readonly conductorURL: string,
  ) {
    const appName = globalParams.appName;
    const cleanConductorURL = conductorURL.replace(/\/+$/, '');
    this.url = `${cleanConductorURL}/websocket/${appName}/${conductorKey}`;
  }

  resetWebsocket() {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = undefined;
    }
    if (this.pingTimeout) {
      clearTimeout(this.pingTimeout);
      this.pingTimeout = undefined;
    }
    if (this.websocket) {
      this.websocket.terminate(); // Terminate the existing connection
      this.websocket = undefined; // Set the websocket to undefined to indicate it's closed
    }
    this.scheduleReconnect();
  }

  scheduleReconnect() {
    if (this.reconnectTimeout || this.isShuttingDown) {
      return;
    }
    this.dbosExec.logger.debug(`Reconnecting in ${this.reconnectDelayMs / 1000} second`);
    this.reconnectTimeout = setTimeout(() => {
      this.reconnectTimeout = undefined;
      this.dispatchLoop();
    }, this.reconnectDelayMs);
  }

  dispatchLoop() {
    if (this.websocket) {
      this.dbosExec.logger.warn('Conductor websocket already exists');
      return;
    }

    if (this.isShuttingDown) {
      this.dbosExec.logger.debug('Not starting dispatch loop as conductor is shutting down');
      return;
    }

    try {
      this.dbosExec.logger.debug(`Connecting to conductor at ${this.url}`);
      // Start a new websocket connection
      this.websocket = new WebSocket(this.url, { handshakeTimeout: 5000 });
      this.websocket.on('open', () => {
        this.dbosExec.logger.debug('Opened connection to DBOS conductor');
        this.pingInterval = setInterval(() => {
          if (this.websocket?.readyState !== WebSocket.OPEN) {
            return;
          }
          this.dbosExec.logger.debug('Sending ping to conductor');
          this.websocket.ping();
          // Set ping timeout.
          this.pingTimeout = setTimeout(() => {
            if (this.isShuttingDown) {
              this.isClosed = true;
              return;
            }
            // Otherwise, try to reconnect
            this.dbosExec.logger.error('Connection to conductor lost. Reconnecting...');
            this.resetWebsocket();
          }, this.pingTimeoutMs);
        }, this.pingPeriodMs);
      });

      this.websocket.on('pong', () => {
        this.dbosExec.logger.debug('Received pong from conductor');
        if (this.pingTimeout) {
          clearTimeout(this.pingTimeout);
          this.pingTimeout = undefined;
        }
      });

      this.websocket.on('message', async (data: string) => {
        this.dbosExec.logger.debug(`Received message from conductor: ${data}`);
        const baseMsg = DBOSJSON.parse(data) as protocol.BaseMessage;
        const msgType = baseMsg.type;
        let errorMsg: string | undefined = undefined;
        switch (msgType) {
          case protocol.MessageType.EXECUTOR_INFO:
            const infoResp = new protocol.ExecutorInfoResponse(
              baseMsg.request_id,
              globalParams.executorID,
              globalParams.appVersion,
              hostname(),
            );
            this.websocket!.send(DBOSJSON.stringify(infoResp));
            this.dbosExec.logger.info('Connected to DBOS conductor');
            break;
          case protocol.MessageType.RECOVERY:
            const recoveryMsg = baseMsg as protocol.RecoveryRequest;
            let success = true;
            try {
              await this.dbosExec.recoverPendingWorkflows(recoveryMsg.executor_ids);
            } catch (e) {
              errorMsg = `Exception encountered when recovering workflows: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
              success = false;
            }
            const recoveryResp = new protocol.RecoveryResponse(baseMsg.request_id, success, errorMsg);
            this.websocket!.send(DBOSJSON.stringify(recoveryResp));
            break;
          case protocol.MessageType.CANCEL:
            const cancelMsg = baseMsg as protocol.CancelRequest;
            let cancelSuccess = true;
            try {
              await this.dbosExec.cancelWorkflow(cancelMsg.workflow_id);
            } catch (e) {
              errorMsg = `Exception encountered when cancelling workflow ${cancelMsg.workflow_id}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
              cancelSuccess = false;
            }
            const cancelResp = new protocol.CancelResponse(baseMsg.request_id, cancelSuccess, errorMsg);
            this.websocket!.send(DBOSJSON.stringify(cancelResp));
            break;
          case protocol.MessageType.RESUME:
            const resumeMsg = baseMsg as protocol.ResumeRequest;
            let resumeSuccess = true;
            try {
              await this.dbosExec.resumeWorkflow(resumeMsg.workflow_id);
            } catch (e) {
              errorMsg = `Exception encountered when resuming workflow ${resumeMsg.workflow_id}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
              resumeSuccess = false;
            }
            const resumeResp = new protocol.ResumeResponse(baseMsg.request_id, resumeSuccess, errorMsg);
            this.websocket!.send(DBOSJSON.stringify(resumeResp));
            break;
          case protocol.MessageType.RESTART:
            const restartMsg = baseMsg as protocol.RestartRequest;
            let restartSuccess = true;
            try {
              await this.dbosExec.executeWorkflowUUID(restartMsg.workflow_id, true);
            } catch (e) {
              errorMsg = `Exception encountered when restarting workflow ${restartMsg.workflow_id}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
              restartSuccess = false;
            }
            const restartResp = new protocol.RestartResponse(baseMsg.request_id, restartSuccess, errorMsg);
            this.websocket!.send(DBOSJSON.stringify(restartResp));
            break;
          case protocol.MessageType.LIST_WORKFLOWS:
            const listWFMsg = baseMsg as protocol.ListWorkflowsRequest;
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
            let workflowsOutput: protocol.WorkflowsOutput[] = [];
            try {
              const wfIDs = (await this.dbosExec.systemDatabase.getWorkflows(listWFReq)).workflowUUIDs;
              workflowsOutput = await Promise.all(
                wfIDs.map(async (i) => {
                  const wfInfo = await getWorkflowInfo(this.dbosExec.systemDatabase, i, false);
                  return new protocol.WorkflowsOutput(wfInfo);
                }),
              );
            } catch (e) {
              errorMsg = `Exception encountered when listing workflows: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const wfsResp = new protocol.ListWorkflowsResponse(listWFMsg.request_id, workflowsOutput, errorMsg);
            this.websocket!.send(DBOSJSON.stringify(wfsResp));
            break;
          case protocol.MessageType.LIST_QUEUED_WORKFLOWS:
            const listQueuedWFMsg = baseMsg as protocol.ListQueuedWorkflowsRequest;
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
            let queuedWFOutput: protocol.WorkflowsOutput[] = [];
            try {
              const queuedWFIDs = (await this.dbosExec.systemDatabase.getQueuedWorkflows(listQueuedWFReq))
                .workflowUUIDs;
              queuedWFOutput = await Promise.all(
                queuedWFIDs.map(async (i) => {
                  const wfInfo = await getWorkflowInfo(this.dbosExec.systemDatabase, i, false);
                  return new protocol.WorkflowsOutput(wfInfo);
                }),
              );
            } catch (e) {
              errorMsg = `Exception encountered when listing queued workflows: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const queuedWfsResp = new protocol.ListQueuedWorkflowsResponse(
              listQueuedWFMsg.request_id,
              queuedWFOutput,
              errorMsg,
            );
            this.websocket!.send(DBOSJSON.stringify(queuedWfsResp));
            break;
          case protocol.MessageType.GET_WORKFLOW:
            const getWFMsg = baseMsg as protocol.GetWorkflowRequest;
            let wfOutput: protocol.WorkflowsOutput | undefined = undefined;
            try {
              const wfInfo = await getWorkflowInfo(this.dbosExec.systemDatabase, getWFMsg.workflow_id, false);
              if (wfInfo.workflowUUID) {
                wfOutput = new protocol.WorkflowsOutput(wfInfo);
              }
            } catch (e) {
              errorMsg = `Exception encountered when getting workflow ${getWFMsg.workflow_id}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const getWFResp = new protocol.GetWorkflowResponse(getWFMsg.request_id, wfOutput, errorMsg);
            this.websocket!.send(DBOSJSON.stringify(getWFResp));
            break;
          case protocol.MessageType.EXIST_PENDING_WORKFLOWS:
            const existPendingMsg = baseMsg as protocol.ExistPendingWorkflowsRequest;
            let hasPendingWFs = false;
            try {
              const pendingWFs = await this.dbosExec.systemDatabase.getPendingWorkflows(
                existPendingMsg.executor_id,
                existPendingMsg.application_version,
              );
              hasPendingWFs = pendingWFs.length > 0;
            } catch (e) {
              errorMsg = `Exception encountered when checking for pending workflows: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const existPendingResp = new protocol.ExistPendingWorkflowsResponse(
              baseMsg.request_id,
              hasPendingWFs,
              errorMsg,
            );
            this.websocket!.send(DBOSJSON.stringify(existPendingResp));
            break;
          default:
            this.dbosExec.logger.warn(`Unknown message type: ${baseMsg.type}`);
            // Still need to send a response to the conductor
            const unknownResp = new protocol.BaseResponse(baseMsg.type, baseMsg.request_id, 'Unknown message type');
            this.websocket!.send(DBOSJSON.stringify(unknownResp));
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
          this.resetWebsocket();
        }
      });

      this.websocket.on('error', (err) => {
        console.error(err);
        // TODO: better error message, showing the detailed error.
        this.dbosExec.logger.error(`Unexpected exception in connection to conductor. Reconnecting: ${err.message}`);
        this.resetWebsocket();
      });
    } catch (e) {
      this.dbosExec.logger.error(`Error in conductor loop. Reconnecting: ${(e as Error).message}`);
      this.resetWebsocket();
    }
  }

  stop() {
    this.isShuttingDown = true;
    clearInterval(this.pingInterval);
    clearTimeout(this.pingTimeout);
    clearTimeout(this.reconnectTimeout);
    if (this.websocket) {
      this.websocket.close();
    }
  }
}
