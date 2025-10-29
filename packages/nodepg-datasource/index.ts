// using https://github.com/brianc/node-postgres

import { DBOS, DBOSWorkflowConflictError, FunctionName } from '@dbos-inc/dbos-sdk';
import {
  type DataSourceTransactionHandler,
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  registerTransaction,
  runTransaction,
  PGTransactionConfig,
  DBOSDataSource,
  registerDataSource,
  CheckSchemaInstallationReturn,
  checkSchemaInstallationPG,
} from '@dbos-inc/dbos-sdk/datasource';
import { Client, type ClientBase, type ClientConfig, Pool, type PoolConfig } from 'pg';
import { AsyncLocalStorage } from 'node:async_hooks';
import { SuperJSON } from 'superjson';

interface NodePostgresDataSourceContext {
  client: ClientBase;
  owner: NodePostgresTransactionHandler;
}

interface NodePostgresTransactionOptions extends PGTransactionConfig {
  name?: string;
}

export { NodePostgresTransactionOptions };

const asyncLocalCtx = new AsyncLocalStorage<NodePostgresDataSourceContext>();

class NodePostgresTransactionHandler implements DataSourceTransactionHandler {
  readonly dsType = 'NodePostgresDataSource';
  #poolField: Pool | undefined;
  readonly schemaName: string;

  constructor(
    readonly name: string,
    private readonly config: PoolConfig,
    schemaName: string = 'dbos',
  ) {
    this.schemaName = schemaName;
  }

  async initialize(): Promise<void> {
    const pool = this.#poolField;
    this.#poolField = new Pool(this.config);
    await pool?.end();

    const client = await this.#poolField.connect();

    try {
      let installed = false;
      try {
        const res = await client.query<CheckSchemaInstallationReturn>(checkSchemaInstallationPG(this.schemaName));
        installed = !!res.rows[0].schema_exists && !!res.rows[0].table_exists;
      } catch (e) {
        throw new Error(
          `In initialization of 'NodePostgresDataSource' ${this.name}: Database could not be queried: ${(e as Error).message}`,
        );
      }

      // Install
      if (!installed) {
        try {
          await client.query(createTransactionCompletionSchemaPG(this.schemaName));
          await client.query(createTransactionCompletionTablePG(this.schemaName));
        } catch (err) {
          throw new Error(
            `In initialization of 'NodePostgresDataSource' ${this.name}: The 'dbos.transaction_completion' table does not exist, and could not be created.  This should be added to your database migrations.
            See: https://docs.dbos.dev/typescript/tutorials/transaction-tutorial#installing-the-dbos-schema`,
          );
        }
      }
    } finally {
      client.release();
    }
  }

  async destroy(): Promise<void> {
    const pool = this.#poolField;
    this.#poolField = undefined;
    await pool?.end();
  }

  get #pool(): Pool {
    if (!this.#poolField) {
      throw new Error(`DataSource ${this.name} is not initialized.`);
    }
    return this.#poolField;
  }

  async #checkExecution(
    workflowID: string,
    stepID: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    type Result = { output: string | null; error: string | null };
    const { rows } = await this.#pool.query<Result>(
      /*sql*/
      `SELECT output, error FROM dbos.transaction_completion
       WHERE workflow_id = $1 AND function_num = $2`,
      [workflowID, stepID],
    );
    if (rows.length === 0) {
      return undefined;
    }
    const { output, error } = rows[0];
    return error !== null ? { error } : { output };
  }

  static async #recordOutput(
    client: ClientBase,
    workflowID: string,
    stepID: number,
    output: string | null,
  ): Promise<void> {
    try {
      await client.query(
        /*sql*/
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
      await this.#pool.query(
        /*sql*/
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

  async #transaction<Return>(
    func: (client: ClientBase) => Promise<Return>,
    config: NodePostgresTransactionOptions = {},
  ): Promise<Return> {
    const isolationLevel = config?.isolationLevel ? `ISOLATION LEVEL ${config.isolationLevel}` : '';
    const readOnly = config?.readOnly ?? false;
    const accessMode = config?.readOnly === undefined ? '' : readOnly ? 'READ ONLY' : 'READ WRITE';

    const client = await this.#pool.connect();
    try {
      await client.query(/*sql*/ `BEGIN ${isolationLevel} ${accessMode}`);
      const result = await func(client);
      await client.query(/*sql*/ `COMMIT`);
      return result;
    } catch (error) {
      await client.query(/*sql*/ `ROLLBACK`);
      throw error;
    } finally {
      client.release();
    }
  }

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: NodePostgresTransactionOptions | undefined,
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
        const result = await this.#transaction<Return>(async (client) => {
          // execute user's transaction function
          const result = await asyncLocalCtx.run({ client, owner: this }, async () => {
            return (await func.call(target, ...args)) as Return;
          });

          // save the output of read/write transactions
          if (saveResults) {
            await NodePostgresTransactionHandler.#recordOutput(
              client,
              workflowID,
              stepID!,
              SuperJSON.stringify(result),
            );
          }

          return result;
        }, config);

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

export class NodePostgresDataSource implements DBOSDataSource<NodePostgresTransactionOptions> {
  static #getClient(p?: NodePostgresTransactionHandler): ClientBase {
    if (!DBOS.isInTransaction()) {
      throw new Error('Invalid use of NodePostgresDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('Invalid use of NodePostgresDataSource.client outside of a DBOS transaction.');
    }
    if (p && p !== ctx.owner) {
      throw new Error('Use of `NodePostgresDataSource.client` from the wrong object');
    }
    return ctx.client;
  }

  static get client() {
    return NodePostgresDataSource.#getClient(undefined);
  }

  get client() {
    return NodePostgresDataSource.#getClient(this.#provider);
  }

  static async initializeDBOSSchema(config: ClientConfig, schemaName: string = 'dbos'): Promise<void> {
    const client = new Client(config);
    try {
      await client.connect();
      await client.query(createTransactionCompletionSchemaPG(schemaName));
      await client.query(createTransactionCompletionTablePG(schemaName));
    } finally {
      await client.end();
    }
  }

  #provider: NodePostgresTransactionHandler;

  constructor(
    readonly name: string,
    config: PoolConfig,
    schemaName: string = 'dbos',
  ) {
    this.#provider = new NodePostgresTransactionHandler(name, config, schemaName);
    registerDataSource(this.#provider);
  }

  async runTransaction<T>(func: () => Promise<T>, config?: NodePostgresTransactionOptions) {
    return await runTransaction(func, config?.name ?? func.name, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: NodePostgresTransactionOptions & FunctionName,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, config);
  }

  transaction(config?: NodePostgresTransactionOptions) {
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
