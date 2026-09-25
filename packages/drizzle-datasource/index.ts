import { Client, ClientConfig, Pool, PoolClient, PoolConfig } from 'pg';
import { DBOS, FunctionName } from '@dbos-inc/dbos-sdk';
import {
  type DataSourceTransactionHandler,
  isPGRetriableTransactionError,
  DBOSError,
  DBOSStepAlreadyRecordedError,
  DBOSWorkflowConflictError,
  assertStillOwnsWorkflow,
  replayRecordedStep,
  registerTransaction,
  runTransaction,
  DBOSDataSource,
  registerDataSource,
  DataSourceMigrationOptions,
  DataSourceSQLExecutor,
  initializeDataSourceSchemaPG,
  migrateDataSourcePG,
  verifyDataSourcePG,
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

function drizzleExecutor(db: Pick<NodePgDatabase<{ [key: string]: object }>, 'execute'>): DataSourceSQLExecutor {
  return async (text) => (await db.execute<Record<string, unknown>>(sql.raw(text))).rows;
}

function clientExecutor(client: Pick<Client, 'query'>): DataSourceSQLExecutor {
  return async (text) => (await client.query<Record<string, unknown>>(text)).rows;
}

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
  #connection: DrizzleConnection | undefined;
  readonly schemaName: string;
  readonly #userProvidedPool: boolean;

  constructor(
    readonly name: string,
    private readonly configOrPool: PoolConfig | Pool,
    private readonly entities: { [key: string]: object } = {},
    schemaName: string = 'dbos',
    private readonly options: DataSourceMigrationOptions = {},
  ) {
    this.schemaName = schemaName;
    this.#userProvidedPool = configOrPool instanceof Pool;
  }

  async initialize(): Promise<void> {
    const conn = this.#connection;

    const driver = this.configOrPool instanceof Pool ? this.configOrPool : new Pool(this.configOrPool);
    const db = drizzle({ client: driver, schema: this.entities });
    this.#connection = { db, end: this.#userProvidedPool ? async () => {} : () => driver.end() };
    await conn?.end();

    if (this.options.runMigrations === false) {
      await verifyDataSourcePG(drizzleExecutor(db), this.schemaName, this.name);
      return;
    }

    try {
      await db.transaction((tx) => migrateDataSourcePG(drizzleExecutor(tx), this.schemaName));
    } catch (err) {
      throw new Error(
        `In initialization of 'DrizzleDataSource' ${this.name}: The '${this.schemaName}' transaction schema could not be migrated: ${(err as Error).message}. This should be added to your database migrations.
          See: https://docs.dbos.dev/typescript/tutorials/transaction-tutorial#installing-the-dbos-schema`,
      );
    }
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

  async deleteCheckpoints(workflowID: string, startStep: number, beforeCommit?: () => Promise<void>): Promise<void> {
    await this.#drizzle.transaction(async (client) => {
      await client.execute(sql`
        DELETE FROM ${sql.identifier(this.schemaName)}.transaction_completion
        WHERE workflow_id = ${workflowID} AND function_num >= ${startStep}`);
      await beforeCommit?.();
    });
  }

  async #checkExecution(
    workflowID: string,
    stepID: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    type Result = { output: string | null; error: string | null };

    const statement = sql`
        SELECT output, error FROM ${sql.identifier(this.schemaName)}.transaction_completion
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
    schemaName: string,
  ): Promise<void> {
    const statement = sql`
      INSERT INTO ${sql.identifier(schemaName)}.transaction_completion (workflow_id, function_num, output)
      VALUES (${workflowID}, ${stepID}, ${output})
      ON CONFLICT (workflow_id, function_num) DO NOTHING
      RETURNING workflow_id`;
    const { rows } = await client.execute(statement);
    if (rows.length === 0) {
      throw new DBOSStepAlreadyRecordedError(workflowID, stepID);
    }
    // Holding this step's row, so a later owner's insert waits on our commit.
    await assertStillOwnsWorkflow(workflowID);
  }

  async #recordError(workflowID: string, stepID: number, error: string): Promise<void> {
    const statement = sql`
      INSERT INTO ${sql.identifier(this.schemaName)}.transaction_completion (workflow_id, function_num, error)
      VALUES (${workflowID}, ${stepID}, ${error})
      ON CONFLICT (workflow_id, function_num) DO NOTHING
      RETURNING workflow_id`;
    const { rows } = await this.#drizzle.execute(statement);
    if (rows.length === 0) {
      throw new DBOSStepAlreadyRecordedError(workflowID, stepID);
    }
  }

  // A duplicate execution won the race, so its recorded outcome is the durable one.
  async #replayConflictingStep<Return>(workflowID: string, stepID: number): Promise<Return> {
    const recorded = await this.#checkExecution(workflowID, stepID);
    if (recorded === undefined) {
      throw new DBOSError(
        `Step ${stepID} of workflow ${workflowID} conflicted with a concurrent execution, but no recorded outcome was found`,
      );
    }
    return replayRecordedStep<Return>(recorded);
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
        return replayRecordedStep<Return>(previousResult);
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
              await DrizzleTransactionHandler.#recordOutput(
                client,
                workflowID,
                stepID!,
                SuperJSON.stringify(result),
                this.schemaName,
              );
            }

            return result;
          },
          { accessMode: config?.accessMode, isolationLevel: config?.isolationLevel },
        );

        return result;
      } catch (error) {
        if (saveResults && error instanceof DBOSStepAlreadyRecordedError) {
          return await this.#replayConflictingStep<Return>(workflowID, stepID!);
        }
        // The new owner wins; recording an error here would replay it as this step's outcome.
        if (error instanceof DBOSWorkflowConflictError) throw error;
        if (isPGRetriableTransactionError(error)) {
          DBOS.span?.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMS }, performance.now());
          // Retry serialization failures.
          await new Promise((resolve) => setTimeout(resolve, retryWaitMS));
          retryWaitMS = Math.min(retryWaitMS * backoffFactor, maxRetryWaitMS);
          continue;
        } else {
          if (saveResults) {
            const message = SuperJSON.stringify(error);
            try {
              await this.#recordError(workflowID, stepID!, message);
            } catch (recordError) {
              if (recordError instanceof DBOSStepAlreadyRecordedError) {
                return await this.#replayConflictingStep<Return>(workflowID, stepID!);
              }
              throw recordError;
            }
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

  /**
   * Create or migrate the DBOS transaction schema, typically with a privileged role,
   * optionally granting `applicationRole` the minimal permissions to use it.
   * A provided client runs the statements in its current transaction, if any; a config gets a fresh transaction.
   */
  static async initializeDBOSSchema(
    configOrClient: ClientConfig | Client | PoolClient,
    schemaName: string = 'dbos',
    options: { applicationRole?: string } = {},
  ): Promise<void> {
    if (typeof configOrClient === 'object' && 'query' in configOrClient) {
      await initializeDataSourceSchemaPG(clientExecutor(configOrClient), schemaName, options.applicationRole);
    } else {
      const client = new Client(configOrClient);
      try {
        await client.connect();
        await client.query('BEGIN');
        try {
          await initializeDataSourceSchemaPG(clientExecutor(client), schemaName, options.applicationRole);
          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        }
      } finally {
        await client.end();
      }
    }
  }

  #provider: DrizzleTransactionHandler;

  constructor(
    readonly name: string,
    configOrPool: PoolConfig | Pool,
    entities: { [key: string]: object } = {},
    schemaName: string = 'dbos',
    options: DataSourceMigrationOptions = {},
  ) {
    this.#provider = new DrizzleTransactionHandler(name, configOrPool, entities, schemaName, options);
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
