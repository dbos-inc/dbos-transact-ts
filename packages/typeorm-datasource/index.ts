import { PoolConfig } from 'pg';
import { DBOS, DBOSWorkflowConflictError } from '@dbos-inc/dbos-sdk';
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
  PGTransactionConfig,
} from '@dbos-inc/dbos-sdk/datasource';
import { DataSource, EntityManager } from 'typeorm';
import { AsyncLocalStorage } from 'async_hooks';
import { SuperJSON } from 'superjson';

interface DBOSTypeOrmLocalCtx {
  entityManager: EntityManager;
}

const asyncLocalCtx = new AsyncLocalStorage<DBOSTypeOrmLocalCtx>();

interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

class TypeOrmTransactionHandler implements DataSourceTransactionHandler {
  readonly dsType = 'TypeOrm';
  #dataSourceField: DataSource | undefined;

  constructor(
    readonly name: string,
    private readonly config: PoolConfig,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    private readonly entities: Function[],
  ) {}

  static async createInstance(
    config: PoolConfig,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    entities: Function[],
  ): Promise<DataSource> {
    const ds = new DataSource({
      type: 'postgres',
      entities: entities,
      url: config.connectionString,
      host: config.host,
      port: config.port,
      username: config.user,
      // password: config.password,
      database: config.database,
      // ssl: config.ssl,
      connectTimeoutMS: config.connectionTimeoutMillis,
      poolSize: config.max,
    });
    await ds.initialize();
    return ds;
  }

  async initialize(): Promise<void> {
    const ds = this.#dataSourceField;
    this.#dataSourceField = await TypeOrmTransactionHandler.createInstance(this.config, this.entities);
    await ds?.destroy();
  }

  async destroy(): Promise<void> {
    const ds = this.#dataSourceField;
    this.#dataSourceField = undefined;
    await ds?.destroy();
  }

  get #dataSource() {
    if (!this.#dataSourceField) {
      throw new Error(`DataSource ${this.name} is not initialized.`);
    }
    return this.#dataSourceField;
  }

  async #checkExecution(
    workflowID: string,
    stepID: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    type TxOutputRow = Pick<transaction_completion, 'output' | 'error'>;
    const rows = await this.#dataSource.query<TxOutputRow[]>(
      `SELECT output, error FROM dbos.transaction_completion
       WHERE workflow_id=$1 AND function_num=$2;`,
      [workflowID, stepID],
    );

    if (rows.length !== 1) {
      return undefined;
    }

    if (rows[0].output === null) {
      return undefined;
    }

    const { output, error } = rows[0];
    return error !== null ? { error } : { output };
  }

  static async #recordOutput(
    entityManager: EntityManager,
    workflowID: string,
    stepID: number,
    output: string,
  ): Promise<void> {
    try {
      await entityManager.query(
        `INSERT INTO dbos.transaction_completion (workflow_id, function_num, output)
         VALUES ($1, $2, $3)`,
        [workflowID, stepID, output],
      );
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
      await this.#dataSource.query(
        `INSERT INTO dbos.transaction_completion (workflow_id, function_num, error)
         VALUES ($1, $2, $3)`,
        [workflowID, stepID, error],
      );
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  /* Required by base class */
  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: PGTransactionConfig | undefined,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const workflowID = DBOS.workflowID;
    const stepID = DBOS.stepID;
    if (workflowID !== undefined && stepID === undefined) {
      throw new Error('DBOS.stepID is undefined inside a workflow.');
    }

    const isolationLevel = config?.isolationLevel ?? 'READ COMMITTED';
    const readOnly = config?.readOnly ? true : false;
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
        const result = await this.#dataSource.transaction(isolationLevel, async (entityManager: EntityManager) => {
          if (readOnly) {
            await entityManager.query('SET TRANSACTION READ ONLY');
          }

          const result = await asyncLocalCtx.run({ entityManager: entityManager }, async () => {
            return await func.call(target, ...args);
          });

          // save the output of read/write transactions
          if (saveResults) {
            await TypeOrmTransactionHandler.#recordOutput(
              entityManager,
              workflowID,
              stepID!,
              SuperJSON.stringify(result),
            );
          }

          return result;
        });

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

export class TypeOrmDataSource implements DBOSDataSource<PGTransactionConfig> {
  // User calls this... DBOS not directly involved...
  static get entityManager(): EntityManager {
    if (!DBOS.isInTransaction()) {
      throw new Error('Invalid use of TypeOrmDataSource.entityManager outside of a DBOS transaction');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('Invalid use of TypeOrmDataSource.entityManager outside of a DBOS transaction');
    }

    return ctx.entityManager;
  }

  static async initializeInternalSchema(config: PoolConfig): Promise<void> {
    const ds = await TypeOrmTransactionHandler.createInstance(config, []);
    try {
      await ds.query(createTransactionCompletionSchemaPG);
      await ds.query(createTransactionCompletionTablePG);
    } finally {
      await ds.destroy();
    }
  }

  #provider: TypeOrmTransactionHandler;

  constructor(
    readonly name: string,
    config: PoolConfig,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    entities: Function[],
  ) {
    this.#provider = new TypeOrmTransactionHandler(name, config, entities);
    registerDataSource(this.#provider);
  }

  /**
   * Run `callback` as a transaction against this DataSource
   * @param callback Function to run within a transactional context
   * @param funcName Name to record for the transaction
   * @param config Transaction configuration (isolation, etc)
   * @returns Return value from `callback`
   */
  async runTransaction<T>(callback: () => Promise<T>, funcName: string, config?: PGTransactionConfig) {
    return await runTransaction(callback, funcName, { dsName: this.name, config });
  }

  /**
   * Register function as DBOS transaction, to be called within the context
   *  of a transaction on this data source.
   *
   * @param func Function to wrap
   * @param target Name of function
   * @param config Transaction settings
   * @returns Wrapped function, to be called instead of `func`
   */
  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: PGTransactionConfig,
    name?: string,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, { name: name ?? func.name }, config);
  }

  /**
   * Decorator establishing function as a transaction
   */
  transaction(config?: PGTransactionConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const ds = this;
    return function decorator<This, Args extends unknown[], Return>(
      _target: object,
      propertyKey: PropertyKey,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      if (!descriptor.value) {
        throw new Error('Use of decorator when original method is undefined');
      }

      descriptor.value = ds.registerTransaction(descriptor.value, config, String(propertyKey));

      return descriptor;
    };
  }
}
