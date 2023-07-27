/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from 'pg';
import { OperonError } from './error';

export type OperonTransaction<T extends any[], R> = (ctxt: TransactionContext, ...args: T) => Promise<R>;

export interface TransactionConfig {
  isolationLevel?: string;
  readOnly?: boolean;
}

export class TransactionContext {
  client: PoolClient;

  #functionAborted: boolean = false;
  readonly functionID: number;
  readonly readOnly : boolean;
  readonly isolationLevel: string;
  readonly #isolationLevels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE'];

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
    } else if (this.#isolationLevels.includes(config.isolationLevel.toUpperCase())) {
      this.isolationLevel = config.isolationLevel;
    } else {
      throw(new OperonError(`Invalid isolation level: ${config.isolationLevel}`));
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