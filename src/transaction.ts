/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from 'pg';
import { OperonError } from './error';
import { OperationContext } from './context';

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

export class TransactionContext extends OperationContext {
  readonly _contextType = 'TransactionContext';
  #functionAborted: boolean = false;
  readonly readOnly : boolean;
  readonly isolationLevel;

  constructor(
      workflowName: string,
      rolesThatCanRun: string[],
      workflowUUID: string,
      runAs: string,
      readonly client: PoolClient,
      readonly functionID: number,
      readonly functionName: string,
      config: TransactionConfig) {

    super(workflowName, rolesThatCanRun, workflowUUID, runAs);
    if (config.readOnly) {
      this.readOnly = config.readOnly;
    } else {
      this.readOnly = false;
    }
    if (config.isolationLevel) {
      // We already validated the isolation level during config time.
      this.isolationLevel = config.isolationLevel;
    } else {
      this.isolationLevel = 'SERIALIZABLE';
    }
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

  isAborted() : boolean {
    return this.#functionAborted;
  }
}