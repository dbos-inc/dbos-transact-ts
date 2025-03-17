import { DBOSExecutor } from '../dbos-executor';
import { DBOSJSON, globalParams } from '../utils';
import WebSocket from 'ws';
import { BaseMessage, ExecutorInfoResponse, MessageType } from './protocol';

export interface ConductorParams {
  conductorURL?: string;
  conductorKey: string;
}
export class Conductor {
  url: string;
  websocket: WebSocket | undefined = undefined;

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

      this.websocket.on('message', (data: string) => {
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
            this.websocket?.send(DBOSJSON.stringify(infoResp));
            this.dbosExec.logger.info('Connected to DBOS conductor');
            break;
          default:
            this.dbosExec.logger.error(`Unknown message type: ${msgType}`);
        }
      });

      this.websocket.on('close', () => {
        this.dbosExec.logger.info('Conductor connection terminated');
      });

      this.websocket.on('error', (err) => {
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
    if (this.websocket) {
      this.websocket.close();
    }
  }
}
