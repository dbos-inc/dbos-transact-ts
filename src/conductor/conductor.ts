import { DBOSExecutor } from '../dbos-executor';
import { globalParams, sleepms } from '../utils';

export interface ConductorParams {
  conductorURL: string;
  conductorKey: string;
}
export class Conductor {
  url: string;
  private isRunning: boolean = false;
  private interruptResolve?: () => void;

  constructor(
    readonly dbosExec: DBOSExecutor,
    readonly params: ConductorParams,
  ) {
    const appName = globalParams.appName;
    const cleanConductorURL = params.conductorURL.replace(/\/+$/, '');
    this.url = `${cleanConductorURL}/websocket/${appName}/${params.conductorKey}`;
  }

  async dispatchLoop(): Promise<void> {
    this.isRunning = true;
    while (this.isRunning) {
      try {
        this.dbosExec.logger.info(`Connecting to conductor at ${this.url}`);
        await sleepms(1000);
      } catch (e) {
        this.dbosExec.logger.error(`Error in conductor loop: ${(e as Error).message}`);
      }
    }
  }

  stop() {
    if (!this.isRunning) return;
    this.isRunning = false;
    if (this.interruptResolve) {
      this.interruptResolve();
    }
  }
}
