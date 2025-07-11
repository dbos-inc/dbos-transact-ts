// using https://github.com/knex/knex

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
} from '@dbos-inc/dbos-sdk/datasource';
import { AsyncLocalStorage } from 'async_hooks';
import { SuperJSON } from 'superjson';

type PrismaLike = {
  $connect: () => Promise<void>;
  $disconnect: () => Promise<void>;
  $queryRawUnsafe<T = unknown>(query: unknown, ...values: unknown[]): Promise<T>;
  $executeRawUnsafe(query: unknown, ...values: unknown[]): Promise<number>;
};

type PrismaLikeTx = {
  $queryRawUnsafe<T = unknown>(query: TemplateStringsArray | string, ...values: unknown[]): Promise<T>;
  $executeRawUnsafe(query: TemplateStringsArray | string, ...values: unknown[]): Promise<number>;
  $transaction: (tf: (tx: PrismaLike) => Promise<unknown>, config?: unknown) => Promise<unknown>;
};

interface PrismaDataSourceContext {
  client: PrismaLike;
  owner: PrismaTransactionHandler;
}

type TransactionIsolationLevel = 'ReadUncommitted' | 'ReadCommitted' | 'RepeatableRead' | 'Serializable';

export type TransactionConfig = {
  isolationLevel?: TransactionIsolationLevel;
  readOnly?: boolean;
  name?: string;
};

const asyncLocalCtx = new AsyncLocalStorage<PrismaDataSourceContext>();

class PrismaTransactionHandler implements DataSourceTransactionHandler {
  readonly dsType = 'PrismaDataSource';

  constructor(
    readonly name: string,
    private readonly prismaAccess: PrismaLike | (() => PrismaLike),
  ) {}

  get #prismaDB() {
    if (!this.prismaAccess) {
      throw new Error(`DataSource ${this.name} is not initialized.`);
    }
    if (typeof this.prismaAccess === 'function') {
      const p = this.prismaAccess();
      if (!p) {
        throw new Error(`DataSource ${this.name} is not initialized.`);
      }
      return p;
    }
    return this.prismaAccess;
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  destroy(): Promise<void> {
    return Promise.resolve();
  }

  async #checkExecution(
    client: PrismaLike,
    workflowID: string,
    stepID: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    type Result = { output: string | null; error: string | null };
    const result = await client.$queryRawUnsafe<Result>(
      `SELECT output, error FROM dbos.transaction_completion
      WHERE workflow_id = $1 AND function_num = $2`,
      workflowID,
      stepID,
    );
    if (result === undefined) {
      return undefined;
    }
    const { output, error } = result;
    return error !== null ? { error } : { output };
  }

  async #recordError(workflowID: string, stepID: number, error: string): Promise<void> {
    try {
      await this.#prismaDB.$executeRawUnsafe(
        `INSERT INTO dbos.transaction_completion (workflow_id, function_num, error) 
        VALUES ($1, $2, $3)`,
        workflowID,
        stepID,
        error,
      );
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  static async #recordOutput(
    client: PrismaLike,
    workflowID: string,
    stepID: number,
    output: string | null,
  ): Promise<void> {
    try {
      await client.$executeRawUnsafe(
        `INSERT INTO dbos.transaction_completion (workflow_id, function_num, output) 
         VALUES ($1, $2, $3)`,
        workflowID,
        stepID,
        output,
      );
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
      const previousResult = saveResults ? await this.#checkExecution(this.#prismaDB, workflowID, stepID!) : undefined;
      if (previousResult) {
        DBOS.span?.setAttribute('cached', true);

        if ('error' in previousResult) {
          throw SuperJSON.parse(previousResult.error);
        }
        return (previousResult.output ? SuperJSON.parse(previousResult.output) : null) as Return;
      }

      try {
        const result = (await (this.#prismaDB as unknown as PrismaLikeTx).$transaction(
          async (client) => {
            // execute user's transaction function
            const result = await asyncLocalCtx.run({ client, owner: this }, async () => {
              return (await func.call(target, ...args)) as Return;
            });

            // save the output of read/write transactions
            if (saveResults) {
              await PrismaTransactionHandler.#recordOutput(client, workflowID, stepID!, SuperJSON.stringify(result));
            }

            return result;
          },
          { isolationLevel: config?.isolationLevel },
        )) as Return;

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

export class PrismaDataSource<PrismaClient> implements DBOSDataSource<TransactionConfig> {
  static #getClient(p?: PrismaTransactionHandler) {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of PrismaDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('invalid use of PrismaDataSource.client outside of a DBOS transaction.');
    }
    if (p && p !== ctx.owner) throw new Error('Request of `PrismaDataSource.client` from the wrong object.');
    return ctx.client;
  }

  get client(): PrismaClient {
    return PrismaDataSource.#getClient(this.#provider) as PrismaClient;
  }

  static async initializeDBOSSchema(prisma: PrismaLike) {
    await prisma.$queryRawUnsafe(createTransactionCompletionSchemaPG);
    await prisma.$queryRawUnsafe(createTransactionCompletionTablePG);
  }

  static async uninitializeDBOSSchema(prisma: PrismaLike) {
    await prisma.$executeRawUnsafe('DROP TABLE IF EXISTS dbos.transaction_completion;');
    await prisma.$executeRawUnsafe('DROP SCHEMA IF EXISTS dbos;');
  }

  #provider: PrismaTransactionHandler;

  constructor(
    readonly name: string,
    prismaAccess: PrismaLike | (() => PrismaLike),
  ) {
    this.#provider = new PrismaTransactionHandler(name, prismaAccess);
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
