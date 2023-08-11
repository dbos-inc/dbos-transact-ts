/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from "pg";
import { OperonError } from "./error";
import { TelemetryCollector, TelemetrySignal} from "./telemetry";
import { WorkflowContext} from "./workflow";

export type OperonTransaction<T extends any[], R> = (ctxt: TransactionContext, ...args: T) => Promise<R>;

export interface TransactionConfig {
  isolationLevel?: string;
  readOnly?: boolean;
}

const isolationLevels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE'];

export function validateTransactionConfig (params: TransactionConfig){
  if (params.isolationLevel && !isolationLevels.includes(params.isolationLevel.toUpperCase())) {
    throw(new OperonError(`Invalid isolation level: ${params.isolationLevel}`));
  }
}

export class TransactionContext {
  #functionAborted: boolean = false;
  readonly readOnly: boolean;
  readonly isolationLevel;

  constructor(
    private readonly workflowContext: WorkflowContext,
    private readonly telemetryCollector: TelemetryCollector,
    readonly client: PoolClient,
    readonly functionID: number,
    readonly functionName: string,
    config: TransactionConfig
  ) {
    if (config.readOnly) {
      this.readOnly = config.readOnly;
    } else {
      this.readOnly = false;
    }
    if (config.isolationLevel) {
      // We already validated the isolation level during config time.
      this.isolationLevel = config.isolationLevel;
    } else {
      this.isolationLevel = "SERIALIZABLE";
    }
  }

  log(severity: string, message: string): void {
    const workflowContext = this.workflowContext;
    const signal: TelemetrySignal = {
      workflowName: workflowContext.workflowName,
      workflowUUID: workflowContext.workflowUUID,
      functionID: this.functionID,
      functionName: this.functionName,
      runAs: workflowContext.runAs,
      timestamp: Date.now(),
      severity: severity,
      logMessage: message,
    };
    this.telemetryCollector.push(signal);
  }

  async rollback() {
    // If this function has already rolled back, we no longer have the client, so just return.
    if (this.#functionAborted) {
      return;
    }
    await this.client.query("ROLLBACK");
    this.#functionAborted = true;
    this.client.release();
  }

  isAborted(): boolean {
    return this.#functionAborted;
  }
}