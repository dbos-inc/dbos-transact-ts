/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from 'pg';
import { OperonError } from './error';

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
  client: PoolClient;

  #functionAborted: boolean = false;
  readonly functionID: number;
  readonly readOnly : boolean;
  readonly isolationLevel;

  constructor(client: PoolClient, functionID: number, config: TransactionConfig) {
    this.client = client;
    this.functionID = functionID;
    if (!config.readOnly) {
      this.readOnly = false;
    } else {
      this.readOnly = config.readOnly;
    }
    if (!config.isolationLevel) {
      this.isolationLevel = 'SERIALIZABLE';
    } else {
      // We already validated the isolation level during config time.
      this.isolationLevel = config.isolationLevel;
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