// The auto-upgrade loop: runs the enterprise package's pass on a timer for the life of a launched DBOS instance.

import type { AutoUpgradePass } from './enterprise';
import { getAllRegisteredFunctions } from './decorators';
import type { GlobalLogger } from './telemetry/logs';
import { waitOrAbort } from './workflow_management';

export const DEFAULT_AUTO_UPGRADE_INTERVAL_MS = 30_000;
export const DEFAULT_AUTO_UPGRADE_GRACE_MS = 0;
export const AUTO_UPGRADE_STOP_TIMEOUT_MS = 5_000;

export interface EnrolledWorkflow {
  className: string;
  name: string;
}

/**
 * The registered workflows eligible for auto-upgrade. With `defaultOn` (the `autoUpgrade`
 * config flag), every workflow takes part unless it opted out; without it, only workflows
 * that opted in through their WorkflowConfig do.
 */
export function autoUpgradableWorkflows(defaultOn: boolean): EnrolledWorkflow[] {
  const enrolled: EnrolledWorkflow[] = [];
  for (const reg of getAllRegisteredFunctions()) {
    if (!reg.workflowConfig) continue;
    const flag = reg.workflowConfig.autoUpgrade;
    if (flag === true || (flag === undefined && defaultOn)) {
      enrolled.push({ className: reg.className, name: reg.name });
    }
  }
  return enrolled;
}

export class AutoUpgradeLoop {
  readonly #abort = new AbortController();
  #ended?: Promise<void> = undefined;

  constructor(
    public upgradePass: AutoUpgradePass,
    readonly intervalMs: number,
    readonly logger: GlobalLogger,
  ) {}

  get running(): boolean {
    return this.#ended !== undefined && !this.#abort.signal.aborted;
  }

  start(): void {
    this.logger.debug(`Starting auto-upgrade loop, running every ${this.intervalMs} ms`);
    this.#ended = this.#run();
  }

  async #run(): Promise<void> {
    const signal = this.#abort.signal;
    while (!signal.aborted) {
      try {
        await this.upgradePass.run(signal);
      } catch (e) {
        this.logger.warn(`Auto-upgrade pass failed: ${(e as Error).message}`);
      }
      await waitOrAbort(this.intervalMs, signal);
    }
  }

  /** Stop ticking, let a pass in flight finish for a bounded time, then close the pass. */
  async stop(): Promise<void> {
    this.#abort.abort();
    if (this.#ended) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const expired = new Promise<'expired'>((resolve) => {
        timer = setTimeout(() => resolve('expired'), AUTO_UPGRADE_STOP_TIMEOUT_MS);
      });
      try {
        if ((await Promise.race([this.#ended, expired])) === 'expired') {
          this.logger.warn('Auto-upgrade pass did not finish within timeout');
        }
      } finally {
        clearTimeout(timer);
      }
    }
    try {
      await this.upgradePass.close();
    } catch (e) {
      this.logger.warn(`Auto-upgrade pass did not close cleanly: ${(e as Error).message}`);
    }
  }
}
