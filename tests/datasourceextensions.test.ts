import { randomUUID } from 'node:crypto';
import { PoolConfig, DatabaseError as PGDatabaseError } from 'pg';
import knex, { Knex } from 'knex';
import { DBOS } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { AsyncLocalStorage } from 'async_hooks';
import {
  DBOSFailedSqlTransactionError,
  DBOSInvalidWorkflowTransitionError,
  DBOSNotRegisteredError,
} from '../src/error';
import { DBOSJSON, sleepms, ValuesOf } from '../src/utils';
import { MethodRegistration } from '../src/decorators';
import { DBOSExecutor, OperationType } from '../src/dbos-executor';
import { assertCurrentWorkflowContext, runWithDSContext } from '../src/context';
import { Span } from '@opentelemetry/sdk-trace-base';
import { SpanStatusCode } from '@opentelemetry/api';

// Data source implementation (to be moved to DBOS core)
interface DBOSTransactionalDataStore {
  name: string;
  get dsType(): string;

  /**
   * Will be called by DBOS during launch.
   * This may be a no-op if the DS is initialized before telling DBOS about the DS at all.
   */
  initialize(): Promise<void>;

  /**
   * Will be called by DBOS during attempt at clean shutdown (generally in testing scenarios).
   */
  destroy(): Promise<void>;

  /**
   * Wrap a function.  This is part of the registration process
   *   This may also do advance wrapping
   *   (DBOS invoke wrapper will also be created outside of this)
   */
  wrapTransactionFunction<This, Args extends unknown[], Return>(
    config: unknown,
    func: (this: This, ...args: Args) => Promise<Return>,
  ): (this: This, ...args: Args) => Promise<Return>;

  /**
   * Invoke a transaction function
   */
  invokeTransactionFunction<This, Args extends unknown[], Return>(
    reg: MethodRegistration<This, Args, Return> | undefined,
    config: unknown,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return>;
}

/*
 * Knex user data access interface
 */

// This stuff is all specific to PG DBs...
//  We are also agnostic about whether there are admin credentials to do this, or not...
//   it can be done elsewhere.
interface ExistenceCheck {
  exists: boolean;
}

export const schemaExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`;
export const txnOutputTableExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'knex_transaction_outputs')`;

export interface transaction_outputs {
  workflow_id: string;
  function_num: number;
  output: string | null;
}

export const createUserDBSchema = `CREATE SCHEMA IF NOT EXISTS dbos;`;

export const userDBSchema = `
  CREATE TABLE IF NOT EXISTS dbos.knex_transaction_outputs (
    workflow_id TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    PRIMARY KEY (workflow_id, function_num)
  );
`;

interface DBOSKnexLocalCtx {
  knexClient: Knex;
}
const asyncLocalCtx = new AsyncLocalStorage<DBOSKnexLocalCtx>();

function getCurrentDSContextStore(): DBOSKnexLocalCtx | undefined {
  return asyncLocalCtx.getStore();
}

function assertCurrentDSContextStore(): DBOSKnexLocalCtx {
  const ctx = getCurrentDSContextStore();
  if (!ctx)
    throw new DBOSInvalidWorkflowTransitionError('Invalid use of `DBOSKnexDS.knexClient` outside of a `transaction`');
  return ctx;
}

/**
 * Configuration for `DBOSKnexDS` functions
 */
export interface KnexTransactionConfig {
  /** Isolation level to request from underlying app database */
  isolationLevel?: IsolationLevel;
  /** If set, request read-only transaction from underlying app database */
  readOnly?: boolean;
}

/** Isolation typically supported by application databases */
export const IsolationLevel = {
  ReadUncommitted: 'READ UNCOMMITTED',
  ReadCommitted: 'READ COMMITTED',
  RepeatableRead: 'REPEATABLE READ',
  Serializable: 'SERIALIZABLE',
} as const;
export type IsolationLevel = ValuesOf<typeof IsolationLevel>;

export class DBOSKnexDS implements DBOSTransactionalDataStore {
  // User will set this up, in this case
  constructor(
    readonly name: string,
    readonly config: PoolConfig,
  ) {}
  knexInstance: Knex | undefined;
  get knex(): Knex {
    if (!this.knexInstance) throw new Error('Not initialized');
    return this.knexInstance;
  }

  // User calls this... DBOS not directly involved...
  static get knexClient(): Knex {
    const ctx = assertCurrentDSContextStore();
    if (!DBOS.isInTransaction())
      throw new DBOSInvalidWorkflowTransitionError('Invalid use of `DBOS.sqlClient` outside of a `transaction`');
    return ctx.knexClient;
  }

  // initializeSchema - this is up to the user to call.  It's not part of DBOS lifecycle
  async initializeSchema(): Promise<void> {
    const knex = this.createInstance();
    try {
      const schemaExists = await knex.raw<{ rows: ExistenceCheck[] }>(schemaExistsQuery);
      if (!schemaExists.rows[0].exists) {
        await knex.raw(createUserDBSchema);
      }
      const txnOutputTableExists = await knex.raw<{ rows: ExistenceCheck[] }>(txnOutputTableExistsQuery);
      if (!txnOutputTableExists.rows[0].exists) {
        await knex.raw(userDBSchema);
      }
    } finally {
      try {
        await knex.destroy();
      } catch (e) {}
    }
  }

  createInstance() {
    const knexConfig: Knex.Config = {
      client: 'postgres',
      connection: {
        connectionString: this.config.connectionString,
        connectionTimeoutMillis: this.config.connectionTimeoutMillis,
      },
      pool: {
        min: 0,
        max: this.config.max,
      },
    };

    return knex(knexConfig);
  }

  async initialize(): Promise<void> {
    this.knexInstance = this.createInstance();

    return Promise.resolve();
  }

  async destroy(): Promise<void> {
    await this.knex.destroy();
  }

  get dsType(): string {
    return 'DBOSKnex';
  }

  async #checkExecution<R>(
    client: Knex,
    workflowID: string,
    funcNum: number,
  ): Promise<
    | {
        res: R;
      }
    | undefined
  > {
    type TxOutputRow = Pick<transaction_outputs, 'output'> & {
      recorded: boolean;
    };

    const { rows } = await client.raw<{ rows: TxOutputRow[] }>(
      `SELECT output
          FROM dbos.knex_transaction_outputs
          WHERE workflow_id=? AND function_num=?;`,
      [workflowID, funcNum],
    );

    if (rows.length !== 1) {
      return undefined;
    }
    return { res: DBOSJSON.parse(rows[1].output) as R };
  }

  async #recordOutput<R>(client: Knex, workflowID: string, funcNum: number, output: R): Promise<void> {
    const serialOutput = DBOSJSON.stringify(output);
    await client.raw<{ rows: transaction_outputs[] }>(
      `INSERT INTO dbos.knex_transaction_outputs (
        workflow_id, function_num,
        output,
        created_at
      ) VALUES (?, ?, ?, ?)`,
      [workflowID, funcNum, serialOutput, Date.now()],
    );
  }

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    _reg: MethodRegistration<This, Args, Return> | undefined,
    config: KnexTransactionConfig | undefined,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    let isolationLevel: Knex.IsolationLevels;
    if (config?.isolationLevel === IsolationLevel.ReadUncommitted) {
      isolationLevel = 'read uncommitted';
    } else if (config?.isolationLevel === IsolationLevel.ReadCommitted) {
      isolationLevel = 'read committed';
    } else if (config?.isolationLevel === IsolationLevel.RepeatableRead) {
      isolationLevel = 'repeatable read';
    } else {
      isolationLevel = 'serializable';
    }

    const readOnly = config?.readOnly ? true : false;

    const wfid = DBOS.workflowID!;
    const funcnum = DBOS.stepID!;
    const funcname = func.name;

    // Retry loop if appropriate
    let retryWaitMillis = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMs = 2000; // Maximum wait 2 seconds.
    let shouldCheckOutput = false;

    while (true) {
      let failedForRetriableReasons = false;
      try {
        const result = await this.knex.transaction<Return>(
          async (transactionClient: Knex.Transaction) => {
            // TODO: serialization duties are based on DB logic here... but not app logic.  Is that right?

            // Check for prior result / error
            // TODO: Question the model here.
            // This is an interesting question, as it fits neither of the 2 common DB patterns
            // Optimistically, checkExection is not necessary on the first trip around,
            //   It can be run on a second iteration if insert has failed.
            // OTOH, to be pessimistic, this should be LOCK / SFU'd

            if (shouldCheckOutput && !readOnly) {
              const executionResult = await this.#checkExecution<Return>(transactionClient, wfid, funcnum);

              if (executionResult) {
                DBOS.span?.setAttribute('cached', true);
                return executionResult.res;
              }
            }

            try {
              const res = await asyncLocalCtx.run({ knexClient: transactionClient }, async () => {
                return await func.call(target, ...args);
              });

              // Save result
              try {
                if (!readOnly) {
                  await this.#recordOutput(transactionClient, wfid, funcnum, res);
                }
              } catch (e) {
                const error = e as Error;
                // Aside from a connectivity error, two kinds of error are anticipated here:
                //  1. The transaction is marked failed, but the user code did not throw.
                //      Bad on them.  We will throw an error (this will get recorded) and not retry.
                //  2. There was a key conflict in the statement, and we need to use the fetched output
                if (this.isFailedSqlTransactionError(error)) {
                  DBOS.logger.error(
                    `In workflow ${wfid}, Postgres aborted a transaction but the function '${funcname}' did not raise an exception.  Please ensure that the transaction method raises an exception if the database transaction is aborted.`,
                  );
                  failedForRetriableReasons = false;
                  throw new DBOSFailedSqlTransactionError(wfid, funcname);
                } else if (this.isKeyConflictError(error)) {
                  // Expected.  There is probably a result to return
                  shouldCheckOutput = true;
                  failedForRetriableReasons = true;
                } else {
                  DBOS.logger.error(`Unexpected error raised in transaction '${funcname}: ${error}`);
                  failedForRetriableReasons = false;
                  throw error;
                }
              }
              return res;
            } catch (e) {
              // There is no reason to record errors.  The system DB does this.
              //   Presumably, the transaction was rolled back and therefore had no side-effects.
              //   There is no reason why you'd get a different error if you re-ran the transaction,
              //    but if you did, that's also presumed to be valid.
              // There's also no suitable transaction to record the error in, so we'd need a new one.
              //   Putting in the sysdb is no different.
              throw e;
            }
          },
          {
            isolationLevel: isolationLevel,
            readOnly: readOnly,
          },
        );
        return result;
      } catch (e) {
        const err = e as Error;
        if (failedForRetriableReasons || this.isRetriableTransactionError(err)) {
          DBOS.span?.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMillis }, performance.now());
          // Retry serialization failures.
          await sleepms(retryWaitMillis);
          retryWaitMillis *= backoffFactor;
          retryWaitMillis = retryWaitMillis < maxRetryWaitMs ? retryWaitMillis : maxRetryWaitMs;
          continue;
        } else {
          throw err;
        }
      }
    }
  }

  wrapTransactionFunction<This, Args extends unknown[], Return>(
    config: unknown,
    func: (this: This, ...args: Args) => Promise<Return>,
  ): (this: This, ...args: Args) => Promise<Return> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const ds = this;
    const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
      return await ds.invokeTransactionFunction(undefined, config as KnexTransactionConfig, this, func, ...rawArgs);
    };

    Object.defineProperty(invokeWrapper, 'name', {
      value: func.name,
    });

    return invokeWrapper;
  }

  // Think of this as part of the API of the specific transaction provider, not
  //  the interface.  It could also be the internals of a decorator.
  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    target: {
      name: string;
    },
    config?: KnexTransactionConfig,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, target, config);
  }

  getPostgresErrorCode(error: unknown): string | null {
    const dbErr: PGDatabaseError = error as PGDatabaseError;
    return dbErr.code ? dbErr.code : null;
  }

  isRetriableTransactionError(error: unknown): boolean {
    return this.getPostgresErrorCode(error) === '40001';
  }

  isKeyConflictError(error: unknown): boolean {
    return this.getPostgresErrorCode(error) === '23505';
  }

  isFailedSqlTransactionError(error: unknown): boolean {
    return this.getPostgresErrorCode(error) === '25P02';
  }
}

// Register data source (user version)
const userDataSources: Map<string, DBOSTransactionalDataStore> = new Map();

// This is the version that needs no existing transaction registration.  Just goes with it.
async function runAsWorkflowTransaction<T>(callback: () => Promise<T>, funcName: string, dsName?: string) {
  if (!DBOS.isWithinWorkflow) {
    throw new DBOSInvalidWorkflowTransitionError(`Invalid call to \`${funcName}\` outside of a workflow`);
  }
  if (!DBOS.isInWorkflow()) {
    throw new DBOSInvalidWorkflowTransitionError(
      `Invalid call to \`${funcName}\` inside a \`step\`, \`transaction\`, or \`procedure\``,
    );
  }
  const dsn = dsName ?? '<default>';
  const ds = userDataSources.get(dsn);
  if (!ds) throw new DBOSNotRegisteredError(dsn, `Transactional Data Source ${dsn} not registered`);

  const wfctx = assertCurrentWorkflowContext();
  const callnum = wfctx.functionIDGetIncrement();

  const span: Span = DBOSExecutor.globalInstance!.tracer.startSpan(
    funcName,
    {
      operationUUID: wfctx.workflowUUID,
      operationType: OperationType.TRANSACTION,
      authenticatedUser: wfctx.authenticatedUser,
      assumedRole: wfctx.assumedRole,
      authenticatedRoles: wfctx.authenticatedRoles,
      // isolationLevel: txnInfo.config.isolationLevel, // TODO: Pluggable
    },
    wfctx.span,
  );

  try {
    const res = DBOSExecutor.globalInstance!.runAsStep<T>(
      async () => {
        return await runWithDSContext(callnum, async () => {
          return await ds.invokeTransactionFunction(undefined, {}, undefined, callback);
        });
      },
      funcName,
      DBOS.workflowID,
      callnum,
    );

    span.setStatus({ code: SpanStatusCode.OK });
    DBOSExecutor.globalInstance!.tracer.endSpan(span);
    return res;
  } catch (err) {
    const e = err as Error;
    span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
    DBOSExecutor.globalInstance!.tracer.endSpan(span);
    throw err;
  }
}

// Transaction wrapper
function registerTransaction<This, Args extends unknown[], Return>(
  dsName: string,
  func: (this: This, ...args: Args) => Promise<Return>,
  target: {
    name: string;
  },
  config?: unknown,
): (this: This, ...args: Args) => Promise<Return> {
  const dsn = dsName ?? '<default>';
  const ds = userDataSources.get(dsn);
  if (!ds) throw new DBOSNotRegisteredError(dsn, `Transactional Data Source ${dsn} not registered`);
  const dsfunc = ds.wrapTransactionFunction(config, func);

  const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
    if (!DBOS.isWithinWorkflow()) {
      throw new DBOSInvalidWorkflowTransitionError(`Call to transaction '${target.name}' outside of a workflow`);
    }

    if (DBOS.isInTransaction() || DBOS.isInStep()) {
      throw new DBOSInvalidWorkflowTransitionError(
        'Invalid call to a `trasaction` function from within a `step` or `transaction`',
      );
    }

    const wfctx = assertCurrentWorkflowContext();
    const callnum = wfctx.functionIDGetIncrement();
    return DBOSExecutor.globalInstance!.runAsStep<Return>(
      async () => {
        return await runWithDSContext(callnum, async () => {
          return await dsfunc.call(this, ...rawArgs);
        });
      },
      target.name,
      DBOS.workflowID,
      callnum,
    );
  };

  Object.defineProperty(invokeWrapper, 'name', {
    value: target.name,
  });
  return invokeWrapper;
}

////
/// App logic to test
////

const config = generateDBOSTestConfig();
const dsa = new DBOSKnexDS('knexA', config.poolConfig);
userDataSources.set('knexA', dsa);

async function txFunctionGuts() {
  expect(DBOS.isInTransaction()).toBe(true);
  expect(DBOS.isWithinWorkflow()).toBe(true);
  const res = await DBOSKnexDS.knexClient.raw<{ rows: { a: string }[] }>("SELECT 'Tx2 result' as a");
  return res.rows[0].a;
}

const txFunc = registerTransaction('knexA', txFunctionGuts, { name: 'MySecondTx' }, {});

async function wfFunctionGuts() {
  // Transaction variant 2: Let DBOS run a code snippet as a step
  const p1 = await runAsWorkflowTransaction(
    async () => {
      return (await DBOSKnexDS.knexClient.raw<{ rows: { a: string }[] }>("SELECT 'My first tx result' as a")).rows[0].a;
    },
    'MyFirstTx',
    'knexA',
  );

  // Transaction variant 1: Use a registered DBOS transaction function
  const p2 = await txFunc();

  return p1 + '|' + p2;
}

// Workflow functions must always be registered before launch; this
//  allows recovery to occur.
const wfFunction = DBOS.registerWorkflow(wfFunctionGuts, {
  name: 'workflow',
});

describe('decoratorless-api-tests', () => {
  beforeAll(async () => {
    await setUpDBOSTestDb(config);
    await dsa.initializeSchema();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    // TODO: Move this to launch
    for (const [_n, ds] of userDataSources) {
      await ds.initialize();
    }
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
    // TODO: Move this to shutdown
    for (const [_n, ds] of userDataSources) {
      await ds.destroy();
    }
  });

  test('bare-tx-wf-functions', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await wfFunction();
      expect(res).toBe('My first tx result|Tx2 result');
    });

    const wfsteps = (await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('MyFirstTx');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].name).toBe('MySecondTx');
  });
});

// Later
// MikroORM example
