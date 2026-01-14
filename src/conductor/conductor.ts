import { DBOSExecutor } from '../dbos-executor';
import { globalParams } from '../utils';
import WebSocket from 'ws';
import * as protocol from './protocol';
import { GetWorkflowsInput, StatusString } from '..';
import { hostname } from 'node:os';
import { globalTimeout } from '../workflow_management';
import assert from 'node:assert';
import * as zlib from 'node:zlib';
import { promisify } from 'node:util';
import type { ExportedWorkflow } from '../system_database';

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

interface IntervalTimeout {
  interval: NodeJS.Timeout | undefined;
  timeout: NodeJS.Timeout | undefined;
}

export class Conductor {
  url: string;
  websocket: WebSocket | undefined = undefined;
  isShuttingDown = false; // Is in the process of shutting down the connection
  isClosed = false; // Has the connection been fully closed
  pingPeriodMs = 20000; // Time in milliseconds to wait before sending a ping message to the conductor
  pingTimeoutMs = 15000; // Time in milliseconds to wait for a response to a ping message before considering the connection dead
  pingIntervalTimeout: IntervalTimeout | undefined = undefined; // Combined interval and timeout for pinging Conductor
  reconnectDelayMs = 1000;
  reconnectTimeout: NodeJS.Timeout | undefined = undefined;

  constructor(
    readonly dbosExec: DBOSExecutor,
    readonly conductorKey: string,
    readonly conductorURL: string,
  ) {
    const appName = dbosExec.appName;
    assert(appName, 'Application name must be set in configuration in order to use DBOS Conductor');
    const cleanConductorURL = conductorURL.replace(/\/+$/, '');
    this.url = `${cleanConductorURL}/websocket/${appName}/${conductorKey}`;
  }

  resetWebsocket(currWebsocket?: WebSocket, currPing?: IntervalTimeout) {
    clearInterval(currPing?.interval);
    clearTimeout(currPing?.timeout);
    if (currWebsocket) {
      currWebsocket.terminate(); // Terminate the existing connection
      if (this.websocket === currWebsocket) {
        // Only clear if it's the same websocket
        this.websocket = undefined;
      }
    }

    if (this.reconnectTimeout || this.isShuttingDown) {
      return;
    }
    this.dbosExec.logger.debug(`Reconnecting in ${this.reconnectDelayMs / 1000} second`);
    this.reconnectTimeout = setTimeout(() => {
      this.dispatchLoop();
    }, this.reconnectDelayMs);
  }

  setPingInterval(currWebsocket: WebSocket, currPing: IntervalTimeout) {
    // Clear any existing ping interval to avoid multiple intervals being set
    clearInterval(currPing.interval);
    clearTimeout(currPing.timeout);
    currPing.interval = setInterval(() => {
      // Set ping timeout before sending ping. This prevents the socket from hanging at a non-open state indefinitely
      currPing.timeout = setTimeout(() => {
        if (this.isShuttingDown) {
          return;
        } else if (this.reconnectTimeout === undefined) {
          // Otherwise, try to reconnect if we haven't already
          this.dbosExec.logger.warn('Connection to conductor lost. Reconnecting...');
          this.resetWebsocket(currWebsocket, currPing);
        }
      }, this.pingTimeoutMs);
      // Only ping if the connection is open
      if (currWebsocket.readyState === WebSocket.OPEN) {
        this.dbosExec.logger.debug('Sending ping to conductor');
        currWebsocket.ping();
      }
    }, this.pingPeriodMs);
  }

  dispatchLoop() {
    if (
      this.websocket &&
      (this.websocket.readyState === WebSocket.OPEN || this.websocket.readyState === WebSocket.CONNECTING)
    ) {
      this.dbosExec.logger.warn('Conductor websocket already exists');
      return;
    }
    clearTimeout(this.pingIntervalTimeout?.timeout);
    clearInterval(this.pingIntervalTimeout?.interval);
    clearTimeout(this.reconnectTimeout);
    this.reconnectTimeout = undefined;

    if (this.isShuttingDown) {
      this.dbosExec.logger.debug('Not starting dispatch loop as conductor is shutting down');
      return;
    }

    try {
      this.dbosExec.logger.debug(`Connecting to conductor at ${this.url}`);
      // Start a new websocket connection
      const currWebsocket = new WebSocket(this.url, { handshakeTimeout: 5000 });
      this.websocket = currWebsocket;

      // Start ping interval
      const currPing: IntervalTimeout = { interval: undefined, timeout: undefined };
      this.setPingInterval(currWebsocket, currPing);
      this.pingIntervalTimeout = currPing;
      currWebsocket.on('open', () => {
        this.dbosExec.logger.debug('Opened connection to DBOS conductor');
        clearTimeout(currPing.timeout);
      });

      currWebsocket.on('pong', () => {
        this.dbosExec.logger.debug('Received pong from conductor');
        clearTimeout(currPing.timeout);
      });

      currWebsocket.on('message', async (data: string) => {
        this.dbosExec.logger.debug(`Received message from conductor: ${data}`);
        const baseMsg = JSON.parse(data) as protocol.BaseMessage;
        const msgType = baseMsg.type;
        let errorMsg: string | undefined = undefined;
        clearTimeout(currPing.timeout);
        switch (msgType) {
          case protocol.MessageType.EXECUTOR_INFO:
            const infoResp = new protocol.ExecutorInfoResponse(
              baseMsg.request_id,
              globalParams.executorID,
              globalParams.appVersion,
              hostname(),
              'typescript',
              globalParams.dbosVersion,
            );
            currWebsocket.send(JSON.stringify(infoResp));
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
            currWebsocket.send(JSON.stringify(recoveryResp));
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
            currWebsocket.send(JSON.stringify(cancelResp));
            break;
          case protocol.MessageType.DELETE:
            const deleteMsg = baseMsg as protocol.DeleteRequest;
            let deleteSuccess = true;
            try {
              await this.dbosExec.deleteWorkflow(deleteMsg.workflow_id, deleteMsg.delete_children ?? false);
            } catch (e) {
              errorMsg = `Exception encountered when deleting workflow ${deleteMsg.workflow_id}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
              deleteSuccess = false;
            }
            const deleteResp = new protocol.DeleteResponse(baseMsg.request_id, deleteSuccess, errorMsg);
            currWebsocket.send(JSON.stringify(deleteResp));
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
            currWebsocket.send(JSON.stringify(resumeResp));
            break;
          case protocol.MessageType.RESTART:
            const restartMsg = baseMsg as protocol.RestartRequest;
            let restartSuccess = true;
            try {
              await this.dbosExec.forkWorkflow(restartMsg.workflow_id, 0);
            } catch (e) {
              errorMsg = `Exception encountered when restarting workflow ${restartMsg.workflow_id}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
              restartSuccess = false;
            }
            const restartResp = new protocol.RestartResponse(baseMsg.request_id, restartSuccess, errorMsg);
            currWebsocket.send(JSON.stringify(restartResp));
            break;
          case protocol.MessageType.FORK_WORKFLOW:
            const forkMsg = baseMsg as protocol.ForkWorkflowRequest;
            let newWorkflowID = forkMsg.body.new_workflow_id;
            try {
              newWorkflowID = await this.dbosExec.forkWorkflow(forkMsg.body.workflow_id, forkMsg.body.start_step, {
                newWorkflowID: newWorkflowID,
                applicationVersion: forkMsg.body.application_version,
              });
            } catch (e) {
              errorMsg = `Exception encountered when forking workflow ${forkMsg.body.workflow_id} to new workflow ${newWorkflowID} on step ${forkMsg.body.start_step}, app version ${forkMsg.body.application_version}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
              newWorkflowID = undefined;
            }
            const forkResp = new protocol.ForkWorkflowResponse(baseMsg.request_id, newWorkflowID, errorMsg);
            currWebsocket.send(JSON.stringify(forkResp));
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
              forkedFrom: body.forked_from,
              limit: body.limit,
              offset: body.offset,
              sortDesc: body.sort_desc,
              loadInput: body.load_input ?? false, // Default to false if not provided
              loadOutput: body.load_output ?? false, // Default to false if not provided
            };
            let workflowsOutput: protocol.WorkflowsOutput[] = [];
            try {
              const workflows = await this.dbosExec.listWorkflows(listWFReq);
              workflowsOutput = workflows.map((wf) => new protocol.WorkflowsOutput(wf));
            } catch (e) {
              errorMsg = `Exception encountered when listing workflows: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const wfsResp = new protocol.ListWorkflowsResponse(listWFMsg.request_id, workflowsOutput, errorMsg);
            currWebsocket.send(JSON.stringify(wfsResp));
            break;
          case protocol.MessageType.LIST_QUEUED_WORKFLOWS:
            const listQueuedWFMsg = baseMsg as protocol.ListQueuedWorkflowsRequest;
            const bodyQueued = listQueuedWFMsg.body;
            const listQueuedWFReq: GetWorkflowsInput = {
              workflowName: bodyQueued.workflow_name,
              startTime: bodyQueued.start_time,
              endTime: bodyQueued.end_time,
              status: bodyQueued.status as (typeof StatusString)[keyof typeof StatusString],
              forkedFrom: bodyQueued.forked_from,
              limit: bodyQueued.limit,
              queueName: bodyQueued.queue_name,
              offset: bodyQueued.offset,
              sortDesc: bodyQueued.sort_desc,
              loadInput: bodyQueued.load_input ?? false, // Default to false if not provided
            };
            let queuedWFOutput: protocol.WorkflowsOutput[] = [];
            try {
              const workflows = await this.dbosExec.listQueuedWorkflows(listQueuedWFReq);
              queuedWFOutput = workflows.map((wf) => new protocol.WorkflowsOutput(wf));
            } catch (e) {
              errorMsg = `Exception encountered when listing queued workflows: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const queuedWfsResp = new protocol.ListQueuedWorkflowsResponse(
              listQueuedWFMsg.request_id,
              queuedWFOutput,
              errorMsg,
            );
            currWebsocket.send(JSON.stringify(queuedWfsResp));
            break;
          case protocol.MessageType.GET_WORKFLOW:
            const getWFMsg = baseMsg as protocol.GetWorkflowRequest;
            let wfOutput: protocol.WorkflowsOutput | undefined = undefined;
            try {
              const workflow = await this.dbosExec.getWorkflowStatus(getWFMsg.workflow_id);
              if (workflow) {
                wfOutput = new protocol.WorkflowsOutput(workflow);
              }
            } catch (e) {
              errorMsg = `Exception encountered when getting workflow ${getWFMsg.workflow_id}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const getWFResp = new protocol.GetWorkflowResponse(getWFMsg.request_id, wfOutput, errorMsg);
            currWebsocket.send(JSON.stringify(getWFResp));
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
            currWebsocket.send(JSON.stringify(existPendingResp));
            break;
          case protocol.MessageType.LIST_STEPS:
            const listStepsMessage = baseMsg as protocol.ListStepsRequest;
            let workflowSteps: protocol.WorkflowSteps[] | undefined = undefined;
            try {
              const stepsInfo = await this.dbosExec.listWorkflowSteps(listStepsMessage.workflow_id);
              workflowSteps = stepsInfo?.map((i) => new protocol.WorkflowSteps(i));
            } catch (e) {
              errorMsg = `Exception encountered when listing steps ${listStepsMessage.workflow_id}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const listStepsResponse = new protocol.ListStepsResponse(
              listStepsMessage.request_id,
              workflowSteps,
              errorMsg,
            );
            currWebsocket.send(JSON.stringify(listStepsResponse));
            break;
          case protocol.MessageType.RETENTION:
            const retentionMessage = baseMsg as protocol.RetentionRequest;
            let retentionSuccess = true;
            try {
              await this.dbosExec.systemDatabase.garbageCollect(
                retentionMessage.body.gc_cutoff_epoch_ms,
                retentionMessage.body.gc_rows_threshold,
              );
              if (retentionMessage.body.timeout_cutoff_epoch_ms) {
                await globalTimeout(this.dbosExec.systemDatabase, retentionMessage.body.timeout_cutoff_epoch_ms);
              }
            } catch (e) {
              retentionSuccess = false;
              errorMsg = `Exception encountered during enforcing retention policy: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const retentionResponse = new protocol.RetentionResponse(
              retentionMessage.request_id,
              retentionSuccess,
              errorMsg,
            );
            currWebsocket.send(JSON.stringify(retentionResponse));
            break;
          case protocol.MessageType.GET_METRICS:
            const getMetricsMessage = baseMsg as protocol.GetMetricsRequest;
            this.dbosExec.logger.debug(
              `Received metrics request for time range ${getMetricsMessage.start_time} to ${getMetricsMessage.end_time}`,
            );
            let metricsData: protocol.MetricDataOutput[] = [];
            if (getMetricsMessage.metric_class === 'workflow_step_count') {
              try {
                const sysMetrics = await this.dbosExec.systemDatabase.getMetrics(
                  getMetricsMessage.start_time,
                  getMetricsMessage.end_time,
                );
                metricsData = sysMetrics.map((m) => new protocol.MetricDataOutput(m.metricType, m.metricName, m.value));
              } catch (e) {
                errorMsg = `Exception encountered when getting metrics: ${(e as Error).message}`;
                this.dbosExec.logger.error(errorMsg);
              }
            } else {
              errorMsg = `Unexpected metric class: ${getMetricsMessage.metric_class}`;
              this.dbosExec.logger.warn(errorMsg);
            }
            const getMetricsResponse = new protocol.GetMetricsResponse(
              getMetricsMessage.request_id,
              metricsData,
              errorMsg,
            );
            currWebsocket.send(JSON.stringify(getMetricsResponse));
            break;
          case protocol.MessageType.EXPORT_WORKFLOW:
            const exportMsg = baseMsg as protocol.ExportWorkflowRequest;
            let serializedWorkflow: string | null = null;
            try {
              const exported = await this.dbosExec.systemDatabase.exportWorkflow(
                exportMsg.workflow_id,
                exportMsg.export_children ?? false,
              );
              if (exported.length > 0) {
                const jsonStr = JSON.stringify(exported);
                const compressed = await gzip(jsonStr);
                serializedWorkflow = compressed.toString('base64');
              }
            } catch (e) {
              errorMsg = `Exception encountered when exporting workflow ${exportMsg.workflow_id}: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
            }
            const exportResp = new protocol.ExportWorkflowResponse(baseMsg.request_id, serializedWorkflow, errorMsg);
            currWebsocket.send(JSON.stringify(exportResp));
            break;
          case protocol.MessageType.IMPORT_WORKFLOW:
            const importMsg = baseMsg as protocol.ImportWorkflowRequest;
            let importSuccess = true;
            try {
              const compressedData = Buffer.from(importMsg.serialized_workflow, 'base64');
              const decompressed = await gunzip(compressedData);
              const workflows = JSON.parse(decompressed.toString()) as ExportedWorkflow[];
              await this.dbosExec.systemDatabase.importWorkflow(workflows);
            } catch (e) {
              errorMsg = `Exception encountered when importing workflow: ${(e as Error).message}`;
              this.dbosExec.logger.error(errorMsg);
              importSuccess = false;
            }
            const importResp = new protocol.ImportWorkflowResponse(baseMsg.request_id, importSuccess, errorMsg);
            currWebsocket.send(JSON.stringify(importResp));
            break;
          default:
            this.dbosExec.logger.warn(`Unknown message type: ${baseMsg.type}`);
            // Still need to send a response to the conductor
            const unknownResp = new protocol.BaseResponse(baseMsg.type, baseMsg.request_id, 'Unknown message type');
            currWebsocket.send(JSON.stringify(unknownResp));
        }
      });

      currWebsocket.on('close', () => {
        if (this.isShuttingDown) {
          this.dbosExec.logger.info('Shutdown Conductor connection');
          return;
        } else if (this.reconnectTimeout === undefined) {
          this.dbosExec.logger.warn('Connection to conductor lost. Reconnecting.');
          this.resetWebsocket(currWebsocket, currPing);
        }
      });

      currWebsocket.on('unexpected-response', (_, res) => {
        this.dbosExec.logger.warn(`Unexpected response from conductor: ${res.statusCode} ${res.statusMessage}}`);
        if (this.reconnectTimeout === undefined) {
          this.resetWebsocket(currWebsocket, currPing);
        }
      });

      currWebsocket.on('error', (err) => {
        this.dbosExec.logger.warn(`Unexpected exception in connection to conductor. Reconnecting: ${err.message}`);
        if (this.reconnectTimeout === undefined) {
          this.resetWebsocket(currWebsocket, currPing);
        }
      });
    } catch (e) {
      this.dbosExec.logger.warn(`Error in conductor loop. Reconnecting: ${(e as Error).message}`);
      if (this.reconnectTimeout === undefined) {
        this.resetWebsocket(this.websocket, this.pingIntervalTimeout);
      }
    }
  }

  stop() {
    this.isShuttingDown = true;
    clearInterval(this.pingIntervalTimeout?.interval);
    clearTimeout(this.pingIntervalTimeout?.timeout);
    clearTimeout(this.reconnectTimeout);
    if (this.websocket) {
      this.websocket.close();
    }
    this.isClosed = true;
  }
}
