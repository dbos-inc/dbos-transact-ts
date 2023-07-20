/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from 'pg';

export type OperonTransaction<T extends any[], R> = (ctxt: TransactionContext, ...args: T) => Promise<R>;

export class TransactionContext {
  client: PoolClient;

  #functionAborted: boolean = false;
  readonly functionID: number;

  constructor(client: PoolClient, functionID: number) {
    this.client = client;
    this.functionID = functionID;
  }

  async rollback() {
    await this.client.query("ROLLBACK");
    this.#functionAborted = true;
    this.client.release();
  }

  isAborted() : boolean {
    return this.#functionAborted;
  }
}