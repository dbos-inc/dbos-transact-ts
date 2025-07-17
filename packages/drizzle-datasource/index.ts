import { Client, ClientConfig, Pool, PoolConfig } from 'pg';
import { DBOS, DBOSWorkflowConflictError, FunctionName } from '@dbos-inc/dbos-sdk';
import {
  type DataSourceTransactionHandler,
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  registerTransaction,
  runTransaction,
  DBOSDataSource,
  registerDataSource,
} from '@dbos-inc/dbos-sdk/datasource';
import { drizzle, NodePgDatabase } from 'drizzle-orm/node-postgres';
import { AsyncLocalStorage } from 'async_hooks';
import { SuperJSON } from 'superjson';
import { PgTransactionConfig } from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';

interface DrizzleLocalCtx {
  client: NodePgDatabase<{ [key: string]: object }>;
  owner: DrizzleTransactionHandler;
}

export type TransactionConfig = Pick<PgTransactionConfig, 'isolationLevel' | 'accessMode'> & { name?: string };

const asyncLocalCtx = new AsyncLocalStorage<DrizzleLocalCtx>();

export interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

interface DrizzleConnection {
  readonly db: NodePgDatabase<{ [key: string]: object }>;
  end(): Promise<void>;
}

class DrizzleTransactionHandler implements DataSourceTransactionHandler {
  readonly dsType = 'drizzle';
  #connection: DrizzleConnection | undefined;

  constructor(
    readonly name: string,
    private readonly config: PoolConfig,
    private readonly entities: { [key: string]: object } = {},
  ) {}

  async initialize(): Promise<void> {
    const conn = this.#connection;

    const driver = new Pool(this.config);
    const db = drizzle(driver, { schema: this.entities });
    this.#connection = { db, end: () => driver.end() };

    await conn?.end();
  }

  async destroy(): Promise<void> {
    const conn = this.#connection;

    this.#connection = undefined;

    await conn?.end();
  }

  get #drizzle(): NodePgDatabase<{ [key: string]: object }> {
    if (!this.#connection) {
      throw new Error(`DataSource ${this.name} is not initialized.`);
    }
    return this.#connection.db;
  }

  async #checkExecution(
    workflowID: string,
    stepID: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    type Result = { output: string | null; error: string | null };

    const statement = sql`
        SELECT output, error FROM dbos.transaction_completion
        WHERE workflow_id = ${workflowID} AND function_num = ${stepID}`;
    const result = await this.#drizzle.execute<Result>(statement);

    if (result.rows.length !== 1) {
      return undefined;
    }

    const { output, error } = result.rows[0];
    return error !== null ? { error } : { output };
  }

  static async #recordOutput(
    client: NodePgDatabase<{ [key: string]: object }>,
    workflowID: string,
    stepID: number,
    output: string,
  ): Promise<void> {
    try {
      const statement = sql`
        INSERT INTO dbos.transaction_completion (workflow_id, function_num, output) 
        VALUES (${workflowID}, ${stepID}, ${output})`;
      await client.execute(statement);
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  async #recordError(workflowID: string, stepID: number, error: string): Promise<void> {
    try {
      const statement = sql`
        INSERT INTO dbos.transaction_completion (workflow_id, function_num, error) 
        VALUES (${workflowID}, ${stepID}, ${error})`;
      await this.#drizzle.execute(statement);
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  /* Invoke a transaction function, called by the framework */
  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: TransactionConfig | undefined,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const workflowID = DBOS.workflowID;
    const stepID = DBOS.stepID;
    if (workflowID !== undefined && stepID === undefined) {
      throw new Error('DBOS.stepID is undefined inside a workflow.');
    }

    const readOnly = config?.accessMode === 'read only' ? true : false;
    const saveResults = !readOnly && workflowID !== undefined;

    // Retry loop if appropriate
    let retryWaitMS = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMS = 2000; // Maximum wait 2 seconds.

    while (true) {
      // Check to see if this tx has already been executed
      const previousResult = saveResults ? await this.#checkExecution(workflowID, stepID!) : undefined;
      if (previousResult) {
        DBOS.span?.setAttribute('cached', true);

        if ('error' in previousResult) {
          throw SuperJSON.parse(previousResult.error);
        }
        return (previousResult.output ? SuperJSON.parse(previousResult.output) : null) as Return;
      }

      try {
        const result = await this.#drizzle.transaction(
          async (client) => {
            // execute user's transaction function
            const result = await asyncLocalCtx.run({ client, owner: this }, async () => {
              return await func.call(target, ...args);
            });

            // save the output of read/write transactions
            if (saveResults) {
              await DrizzleTransactionHandler.#recordOutput(client, workflowID, stepID!, SuperJSON.stringify(result));
            }

            return result;
          },
          { accessMode: config?.accessMode, isolationLevel: config?.isolationLevel },
        );

        return result;
      } catch (error) {
        if (isPGRetriableTransactionError(error)) {
          DBOS.span?.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMS }, performance.now());
          // Retry serialization failures.
          await new Promise((resolve) => setTimeout(resolve, retryWaitMS));
          retryWaitMS = Math.min(retryWaitMS * backoffFactor, maxRetryWaitMS);
          continue;
        } else {
          if (saveResults) {
            const message = SuperJSON.stringify(error);
            await this.#recordError(workflowID, stepID!, message);
          }

          throw error;
        }
      }
    }
  }
}

export class DrizzleDataSource<CT = NodePgDatabase<{ [key: string]: object }>>
  implements DBOSDataSource<TransactionConfig>
{
  // User calls this... DBOS not directly involved...
  static #getClient(p?: DrizzleTransactionHandler): NodePgDatabase<{ [key: string]: object }> {
    if (!DBOS.isInTransaction()) {
      throw new Error('Invalid use of DrizzleDataSource.client outside of a DBOS transaction');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('Invalid use of DrizzleDataSource.client outside of a DBOS transaction');
    }
    if (p && p !== ctx.owner) {
      throw new Error('Invalid retrieval of `DrizzleDataSource.client` from the incorrect object');
    }
    return ctx.client;
  }

  static get client() {
    return DrizzleDataSource.#getClient(undefined);
  }

  get client() {
    return DrizzleDataSource.#getClient(this.#provider) as CT;
  }

  static async initializeDBOSSchema(config: ClientConfig): Promise<void> {
    const client = new Client(config);
    try {
      await client.connect();
      await client.query(createTransactionCompletionSchemaPG);
      await client.query(createTransactionCompletionTablePG);
    } finally {
      await client.end();
    }
  }

  #provider: DrizzleTransactionHandler;

  constructor(
    readonly name: string,
    config: PoolConfig,
    entities: { [key: string]: object } = {},
  ) {
    this.#provider = new DrizzleTransactionHandler(name, config, entities);
    registerDataSource(this.#provider);
  }

  async runTransaction<T>(func: () => Promise<T>, config?: TransactionConfig) {
    return await runTransaction(func, config?.name ?? func.name, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: TransactionConfig & FunctionName,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, config);
  }

  // decorator
  transaction(config?: TransactionConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const ds = this;
    return function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: PropertyKey,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      if (!descriptor.value) {
        throw new Error('Use of decorator when original method is undefined');
      }

      descriptor.value = ds.registerTransaction(descriptor.value, {
        ...config,
        ctorOrProto: target,
        name: config?.name ?? String(propertyKey),
      });

      return descriptor;
    };
  }
}
