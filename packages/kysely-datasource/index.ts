// using https://kysely.dev/
import { DBOS, DBOSWorkflowConflictError, FunctionName } from '@dbos-inc/dbos-sdk';
import {
  type DataSourceTransactionHandler,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  registerTransaction,
  runTransaction,
  DBOSDataSource,
  registerDataSource,
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  CheckSchemaInstallationReturn,
  checkSchemaInstallationPG,
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

class KyselyTransactionHandler implements DataSourceTransactionHandler {
  readonly dsType = 'KyselyDataSource';
  #kyselyDBField: Kysely<DBOSKyselyTables>;

  constructor(
    readonly name: string,
    private readonly poolConfig: PoolConfig,
  ) {
    this.#kyselyDBField = new Kysely<DBOSKyselyTables>({
      dialect: new PostgresDialect({
        pool: new Pool(poolConfig),
      }),
    });
  }

  async initialize(): Promise<void> {
    const kyselyDB = this.#kyselyDBField;
    this.#kyselyDBField = new Kysely<DBOSKyselyTables>({
      dialect: new PostgresDialect({
        pool: new Pool(this.poolConfig),
      }),
    });
    await kyselyDB?.destroy();

    // Check for connectivity & the schema
    let installed = false;
    try {
      const { rows } = await sql
        .raw<CheckSchemaInstallationReturn>(checkSchemaInstallationPG)
        .execute(this.#kyselyDBField);
      const { schema_exists, table_exists } = rows[0];
      installed = !!schema_exists && !!table_exists;
    } catch (e) {
      throw new Error(
        `In initialization of 'KyselyDataSource' ${this.name}: Database could not be reached: ${(e as Error).message}`,
      );
    }

    if (!installed) {
      try {
        await sql.raw(createTransactionCompletionSchemaPG).execute(this.#kyselyDBField);
        await sql.raw(createTransactionCompletionTablePG).execute(this.#kyselyDBField);
      } catch (err) {
        throw new Error(
          `In initialization of 'KyselyDataSource' ${this.name}: The 'dbos.transaction_completion' table does not exist, and could not be created.  This should be added to your database migrations.
            See: https://docs.dbos.dev/typescript/tutorials/transaction-tutorial#installing-the-dbos-schema`,
        );
      }
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
    try {
      await this.#kyselyDB
        .insertInto('dbos.transaction_completion')
        .values({
          workflow_id: workflowID,
          function_num: stepID,
          error,
          output: null,
        })
        .execute();
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  static async #recordOutput(
    client: Transaction<DBOSKyselyTables>,
    workflowID: string,
    stepID: number,
    output: string | null,
  ): Promise<void> {
    try {
      await client
        .insertInto('dbos.transaction_completion')
        .values({
          workflow_id: workflowID,
          function_num: stepID,
          output,
          error: null,
        })
        .execute();
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
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
        DBOS.span?.setAttribute('cached', true);

        if ('error' in previousResult) {
          throw SuperJSON.parse(previousResult.error);
        }
        return (previousResult.output ? SuperJSON.parse(previousResult.output) : null) as Return;
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
        if (isPGRetriableTransactionError(error)) {
          DBOS.span?.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMS }, performance.now());
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

  static async initializeDBOSSchema(poolConfig: PoolConfig) {
    const client = new Kysely({
      dialect: new PostgresDialect({
        pool: new Pool(poolConfig),
      }),
    });
    await sql.raw(createTransactionCompletionSchemaPG).execute(client);
    await sql.raw(createTransactionCompletionTablePG).execute(client);
    await client.destroy();
  }

  static async uninitializeDBOSSchema(poolConfig: PoolConfig) {
    const client = new Kysely({
      dialect: new PostgresDialect({
        pool: new Pool(poolConfig),
      }),
    });
    await sql
      .raw('DROP TABLE IF EXISTS dbos.transaction_completion; DROP SCHEMA IF EXISTS dbos CASCADE;')
      .execute(client);
    await client.destroy();
  }

  #provider: KyselyTransactionHandler;

  constructor(
    readonly name: string,
    poolConfig: PoolConfig,
  ) {
    this.#provider = new KyselyTransactionHandler(name, poolConfig);
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
