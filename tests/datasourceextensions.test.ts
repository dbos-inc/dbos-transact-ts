import { randomUUID } from 'node:crypto';
import { PoolConfig, DatabaseError as PGDatabaseError } from 'pg';
import knex, { Knex } from 'knex';
import { DBOS } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { AsyncLocalStorage } from 'async_hooks';
import { DBOSInvalidWorkflowTransitionError, DBOSNotRegisteredError } from '../src/error';
import { ValuesOf } from '../src/utils';
import { MethodRegistration } from '../src/decorators';
import { DBOSExecutor } from '../src/dbos-executor';
import { assertCurrentWorkflowContext, getCurrentContextStore, runWithDSContext } from '../src/context';

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
export const txnOutputTableExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'transaction_outputs')`;
export const txnOutputIndexExistsQuery = `SELECT EXISTS (SELECT FROM pg_indexes WHERE schemaname='dbos' AND tablename = 'transaction_outputs' AND indexname = 'transaction_outputs_created_at_index')`;

export interface transaction_outputs {
  workflow_uuid: string;
  functionID: number;
  output: string | null;
  error: string | null;
  txn_id: string | null;
  txn_snapshot: string;
  function_name: string;
}

export const createUserDBSchema = `CREATE SCHEMA IF NOT EXISTS dbos;`;

export const userDBSchema = `
  CREATE TABLE IF NOT EXISTS dbos.transaction_outputs (
    workflow_uuid TEXT NOT NULL,
    functionID INT NOT NULL,
    output TEXT,
    error TEXT,
    txn_id TEXT,
    txn_snapshot TEXT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    function_name TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (workflow_uuid, functionID)
  );
`;

export const userDBIndex = `
  CREATE INDEX IF NOT EXISTS transaction_outputs_created_at_index ON dbos.transaction_outputs (created_at);
`;

export const columnExistsQuery = `
  SELECT EXISTS (
    SELECT FROM information_schema.columns 
    WHERE table_schema = 'dbos' 
      AND table_name = 'transaction_outputs' 
      AND column_name = 'function_name'
  ) AS exists;
`;

export const addColumnQuery = `
  ALTER TABLE dbos.transaction_outputs 
    ADD COLUMN function_name TEXT NOT NULL DEFAULT '';
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
    const schemaExists = await this.knex.raw<{ rows: ExistenceCheck[] }>(schemaExistsQuery);
    if (!schemaExists.rows[0].exists) {
      await this.knex.raw(createUserDBSchema);
    }
    const txnOutputTableExists = await this.knex.raw<{ rows: ExistenceCheck[] }>(txnOutputTableExistsQuery);
    if (!txnOutputTableExists.rows[0].exists) {
      await this.knex.raw(userDBSchema);
    } else {
      const columnExists = await this.knex.raw<{ rows: ExistenceCheck[] }>(columnExistsQuery);
      if (!columnExists.rows[0].exists) {
        await this.knex.raw(addColumnQuery);
      }
    }

    const txnIndexExists = await this.knex.raw<{ rows: ExistenceCheck[] }>(txnOutputIndexExistsQuery);
    if (!txnIndexExists.rows[0].exists) {
      await this.knex.raw(userDBIndex);
    }
  }

  async initialize(): Promise<void> {
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

    this.knexInstance = knex(knexConfig);

    return Promise.resolve();
  }

  async destroy(): Promise<void> {
    await this.knex.destroy();
  }

  get dsType(): string {
    return 'DBOSKnex';
  }

  // TODO: This is in charge of retrying retriable errors
  // TODO: Put the transaction sentinel in here,
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
    const result = await this.knex.transaction<Return>(
      async (transactionClient: Knex.Transaction) => {
        return asyncLocalCtx.run({ knexClient: transactionClient }, async () => {
          return await func.call(target, ...args);
        });
      },
      { isolationLevel: isolationLevel },
    );
    return result;
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
  return DBOSExecutor.globalInstance!.runAsStep<T>(
    async () => {
      return await runWithDSContext(getCurrentContextStore()!.curStepFunctionId!, async () => {
        return await ds.invokeTransactionFunction(undefined, {}, undefined, callback);
      });
    },
    funcName,
    DBOS.workflowID,
    callnum,
  );
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
      return Promise.resolve('My first tx result');
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
