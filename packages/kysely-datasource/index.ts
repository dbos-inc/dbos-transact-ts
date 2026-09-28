/* eslint-disable @typescript-eslint/no-explicit-any */
// using https://kysely.dev/
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
import { AsyncLocalStorage } from 'async_hooks';
import { Kysely, sql, Transaction, IsolationLevel, PostgresDialect } from 'kysely';
import { Pool, PoolConfig } from 'pg';
import { SuperJSON } from 'superjson';

export interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

// Define a database interface for tables used in this datasource
interface DBOSKyselyTables {
  'dbos.transaction_completion': transaction_completion;
}

export interface TransactionConfig {
  name?: string;
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
}

interface KyselyDataSourceContext<DB> {
  client: Transaction<DB>;
  owner: KyselyTransactionHandler;
}

const asyncLocalCtx = new AsyncLocalStorage();

function kyselyExecutor(db: Kysely<any>): DataSourceSQLExecutor {
  return async (text) => (await sql.raw<Record<string, unknown>>(text).execute(db)).rows;
}

class KyselyTransactionHandler implements DataSourceTransactionHandler {
  #kyselyDBField: Kysely<DBOSKyselyTables>;
  readonly schemaName: string;
  readonly poolConfig: PoolConfig | undefined;

  constructor(
    readonly name: string,
    poolConfigOrKysely: PoolConfig | Kysely<any>,
    schemaName: string = 'dbos',
    private readonly options: DataSourceMigrationOptions = {},
  ) {
    this.schemaName = schemaName;

    if (poolConfigOrKysely instanceof Kysely) {
      this.#kyselyDBField = poolConfigOrKysely as Kysely<DBOSKyselyTables>;
    } else {
      this.poolConfig = poolConfigOrKysely;
      this.#kyselyDBField = new Kysely<DBOSKyselyTables>({
        dialect: new PostgresDialect({
          pool: new Pool(poolConfigOrKysely),
        }),
      });
    }
  }

  async initialize(): Promise<void> {
    if (this.poolConfig) {
      const kyselyDB = this.#kyselyDBField;
      this.#kyselyDBField = new Kysely<DBOSKyselyTables>({
        dialect: new PostgresDialect({
          pool: new Pool(this.poolConfig),
        }),
      });
      await kyselyDB?.destroy();
    }

    if (this.options.runMigrations === false) {
      await verifyDataSourcePG(kyselyExecutor(this.#kyselyDBField), this.schemaName, this.name);
      return;
    }

    try {
      await this.#kyselyDBField
        .transaction()
        .execute((trx) => migrateDataSourcePG(kyselyExecutor(trx), this.schemaName));
    } catch (err) {
      throw new Error(
        `In initialization of 'KyselyDataSource' ${this.name}: The '${this.schemaName}' transaction schema could not be migrated: ${(err as Error).message}. This should be added to your database migrations.
            See: https://docs.dbos.dev/typescript/tutorials/transaction-tutorial#installing-the-dbos-schema`,
      );
    }
  }

  async destroy(): Promise<void> {
    await this.#kyselyDBField.destroy();
  }

  get #kyselyDB() {
    if (!this.#kyselyDBField) {
      throw new Error(`DataSource ${this.name} is not initialized.`);
    }
    return this.#kyselyDBField;
  }

  async deleteCheckpoints(workflowID: string, startStep: number, beforeCommit?: () => Promise<void>): Promise<void> {
    await this.#kyselyDB.transaction().execute(async (client) => {
      await client
        .deleteFrom('dbos.transaction_completion')
        .where('workflow_id', '=', workflowID)
        .where('function_num', '>=', startStep)
        .execute();
      await beforeCommit?.();
    });
  }

  async #checkExecution(
    workflowID: string,
    stepID: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    const result = await this.#kyselyDB
      .selectFrom('dbos.transaction_completion')
      .select(['output', 'error'])
      .where('workflow_id', '=', workflowID)
      .where('function_num', '=', stepID)
      .executeTakeFirst();
    if (result === undefined) {
      return undefined;
    }
    const { output, error } = result;
    return error !== null ? { error } : { output };
  }

  async #recordError(workflowID: string, stepID: number, error: string): Promise<void> {
    const inserted = await this.#kyselyDB
      .insertInto('dbos.transaction_completion')
      .values({
        workflow_id: workflowID,
        function_num: stepID,
        error,
        output: null,
      })
      .onConflict((oc) => oc.columns(['workflow_id', 'function_num']).doNothing())
      .returning('workflow_id')
      .executeTakeFirst();
    if (inserted === undefined) {
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

  static async #recordOutput(
    client: Transaction<DBOSKyselyTables>,
    workflowID: string,
    stepID: number,
    output: string | null,
  ): Promise<void> {
    const inserted = await client
      .insertInto('dbos.transaction_completion')
      .values({
        workflow_id: workflowID,
        function_num: stepID,
        output,
        error: null,
      })
      .onConflict((oc) => oc.columns(['workflow_id', 'function_num']).doNothing())
      .returning('workflow_id')
      .executeTakeFirst();
    if (inserted === undefined) {
      throw new DBOSStepAlreadyRecordedError(workflowID, stepID);
    }
    // Holding this step's row, so a later owner's insert waits on our commit.
    await assertStillOwnsWorkflow(workflowID);
  }

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

    const readOnly = config?.readOnly ?? false;
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
        let trx = this.#kyselyDB.transaction();
        if (config?.readOnly) {
          trx = trx.setAccessMode('read only');
        }
        if (config?.isolationLevel) {
          trx = trx.setIsolationLevel(config.isolationLevel);
        }
        const result = await trx.execute(async (client) => {
          // execute user's transaction function
          const result = await asyncLocalCtx.run({ client, owner: this }, async () => {
            return (await func.call(target, ...args)) as Return;
          });

          // save the output of read/write transactions
          if (saveResults) {
            await KyselyTransactionHandler.#recordOutput(client, workflowID, stepID!, SuperJSON.stringify(result));
          }

          return result;
        });

        return result;
      } catch (error) {
        if (saveResults && error instanceof DBOSStepAlreadyRecordedError) {
          return await this.#replayConflictingStep<Return>(workflowID, stepID!);
        }
        // The new owner wins; recording an error here would replay it as this step's outcome.
        if (error instanceof DBOSWorkflowConflictError) throw error;
        if (isPGRetriableTransactionError(error)) {
          DBOS.span?.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMS }, performance.now());
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

export class KyselyDataSource<DB> implements DBOSDataSource<TransactionConfig> {
  static #getClient<DB>(p?: KyselyTransactionHandler): Kysely<DB> {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of KyselyDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore() as KyselyDataSourceContext<DB>;
    if (!ctx) {
      throw new Error('invalid use of KyselyDataSource.client outside of a DBOS transaction.');
    }
    if (p && p !== ctx.owner) throw new Error('Request of `KyselyDataSource.client` from the wrong object.');
    return ctx.client;
  }

  get client(): Kysely<DB> {
    return KyselyDataSource.#getClient(this.#provider);
  }

  /**
   * Create or migrate the DBOS transaction schema, typically with a privileged role,
   * optionally granting `applicationRole` the minimal permissions to use it.
   */
  static async initializeDBOSSchema(
    poolConfig: PoolConfig,
    schemaName: string = 'dbos',
    options: { applicationRole?: string } = {},
  ) {
    const client = new Kysely({
      dialect: new PostgresDialect({
        pool: new Pool(poolConfig),
      }),
    });
    try {
      await client
        .transaction()
        .execute((trx) => initializeDataSourceSchemaPG(kyselyExecutor(trx), schemaName, options.applicationRole));
    } finally {
      await client.destroy();
    }
  }

  static async uninitializeDBOSSchema(poolConfig: PoolConfig, schemaName: string = 'dbos') {
    const client = new Kysely({
      dialect: new PostgresDialect({
        pool: new Pool(poolConfig),
      }),
    });
    await sql
      .raw(
        `DROP TABLE IF EXISTS "${schemaName}".transaction_completion; DROP SCHEMA IF EXISTS "${schemaName}" CASCADE;`,
      )
      .execute(client);
    await client.destroy();
  }

  #provider: KyselyTransactionHandler;

  constructor(
    readonly name: string,
    poolConfigOrKysely: PoolConfig | Kysely<any>,
    schemaName: string = 'dbos',
    options: DataSourceMigrationOptions = {},
  ) {
    this.#provider = new KyselyTransactionHandler(name, poolConfigOrKysely, schemaName, options);
    registerDataSource(this.#provider);
  }

  async runTransaction<R>(func: () => Promise<R>, config?: TransactionConfig) {
    return await runTransaction(func, config?.name ?? func.name, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: TransactionConfig & FunctionName,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, config);
  }

  transaction(config?: TransactionConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const ds = this;
    return function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: PropertyKey,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      if (!descriptor.value) {
        throw Error('Use of decorator when original method is undefined');
      }

      descriptor.value = ds.registerTransaction(descriptor.value, {
        ...config,
        name: config?.name ?? String(propertyKey),
        ctorOrProto: target,
      });

      return descriptor;
    };
  }
}
