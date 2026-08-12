import { randomUUID } from 'node:crypto';
import { Client, Pool, PoolConfig } from 'pg';
import knex, { Knex } from 'knex';
import { SuperJSON } from 'superjson';
import { DBOS, FunctionName } from '../src';

import {
  type DataSourceTransactionHandler,
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  isPGFailedSqlTransactionError,
  registerTransaction,
  runTransaction,
  PGIsolationLevel as IsolationLevel,
  type PGTransactionConfig,
  DBOSDataSource,
  DBOSError,
  DBOSStepAlreadyRecordedError,
  registerDataSource,
  replayRecordedStep,
} from '../src/datasource';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { AsyncLocalStorage } from 'async_hooks';
import { DBOSNotAuthorizedError, DBOSInvalidWorkflowTransitionError } from '../src/error';
import { sleepms } from '../src/utils';
import { DBOSJSON } from '../src/serialization';
import { DBOSExecutor } from '../src/dbos-executor';
import type { WorkflowStatusInternal } from '../src/system_database';

/*
 * Knex user data access interface
 */
type KnexTransactionConfig = PGTransactionConfig & { name?: string };

// This stuff is all specific to PG DBs...
//  We are also agnostic about whether there are admin credentials to do this, or not...
//   it can be done elsewhere.
interface ExistenceCheck {
  exists: boolean;
}

export function schemaExistsQuery(schemaName: string = 'dbos'): string {
  return `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = '${schemaName}')`;
}
export function txnOutputTableExistsQuery(schemaName: string = 'dbos'): string {
  return `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '${schemaName}' AND table_name = 'transaction_completion')`;
}

export interface transaction_outputs {
  workflow_id: string;
  function_num: number;
  output: string | null;
}

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

class KnexDSTH implements DataSourceTransactionHandler {
  constructor(
    readonly name: string,
    readonly config: PoolConfig,
  ) {}
  knexInstance: Knex | undefined;

  async initialize(): Promise<void> {
    this.knexInstance = this.createInstance();

    return Promise.resolve();
  }

  async destroy(): Promise<void> {
    await this.knexInstance?.destroy();
    this.knexInstance = undefined;
  }

  get dsType(): string {
    return 'DBOSKnex';
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

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
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
            // We are using DBOSJSON for this unit test.  Real clients are suggested to use SuperJSON.

            // Check for prior result / error
            // Concurrency is an interesting question
            //   Optimistically, checkExection is not necessary on the first trip around,
            //     It can be run on a second iteration if insert has failed.
            // OTOH, to be pessimistic, this should be LOCK / SELECT FOR UPDATE'd

            if (shouldCheckOutput && !readOnly && wfid) {
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

              // Save result if not read-only, and in workflow
              try {
                if (!readOnly && wfid) {
                  await this.#recordOutput(transactionClient, wfid, funcnum, res);
                }
              } catch (e) {
                const error = e as Error;
                // Aside from a connectivity error, two kinds of error are anticipated here:
                //  1. The transaction is marked failed, but the user code did not throw.
                //      Bad on them.  We will throw an error (this will get recorded) and not retry.
                //  2. There was a key conflict in the statement, and we need to use the fetched output
                if (isPGFailedSqlTransactionError(error)) {
                  DBOS.logger.error(
                    `In workflow ${wfid}, Postgres aborted a transaction but the function '${funcname}' did not raise an exception.  Please ensure that the transaction method raises an exception if the database transaction is aborted.`,
                  );
                  failedForRetriableReasons = false;
                  throw new Error(`Failed: ${wfid}, ${funcname}`);
                } else if (isPGKeyConflictError(error)) {
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
              // We shoud record errors.  That was not implemented here since this is just a unit test,
              //   not a production DS
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
        if (failedForRetriableReasons || isPGRetriableTransactionError(err)) {
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
          FROM dbos.transaction_completion
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
      `INSERT INTO dbos.transaction_completion (
        workflow_id, function_num,
        output,
        created_at
      ) VALUES (?, ?, ?, ?)`,
      [workflowID, funcNum, serialOutput, Date.now()],
    );
  }

  get knex(): Knex {
    if (!this.knexInstance) throw new Error('Not initialized');
    return this.knexInstance;
  }
}

export class DBOSKnexDS implements DBOSDataSource<KnexTransactionConfig> {
  #provider: KnexDSTH;

  // User will set this up, in this case
  constructor(
    readonly name: string,
    readonly config: PoolConfig,
  ) {
    this.#provider = new KnexDSTH(name, config);
    registerDataSource(this.#provider);
  }

  get knex(): Knex {
    return this.#provider.knex;
  }

  // User calls this... DBOS not directly involved...
  static get knexClient(): Knex {
    const ctx = assertCurrentDSContextStore();
    if (!DBOS.isInTransaction())
      throw new DBOSInvalidWorkflowTransitionError('Invalid use of `DBOS.sqlClient` outside of a `transaction`');
    return ctx.knexClient;
  }

  // initializeDBOSSchema - this is up to the user to call.  It's not part of DBOS lifecycle
  async initializeDBOSSchema(): Promise<void> {
    const knex = this.#provider.createInstance();
    const schemaName = 'dbos'; // Use default schema name for tests
    try {
      const schemaExists = await knex.raw<{ rows: ExistenceCheck[] }>(schemaExistsQuery(schemaName));
      if (!schemaExists.rows[0].exists) {
        await knex.raw(createTransactionCompletionSchemaPG(schemaName));
      }
      const txnOutputTableExists = await knex.raw<{ rows: ExistenceCheck[] }>(txnOutputTableExistsQuery(schemaName));
      if (!txnOutputTableExists.rows[0].exists) {
        await knex.raw(createTransactionCompletionTablePG(schemaName));
      }
    } finally {
      try {
        await knex.destroy();
      } catch (e) {}
    }
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: KnexTransactionConfig & FunctionName,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, config);
  }

  static registerTransaction<This, Args extends unknown[], Return>(
    dsname: string,
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: KnexTransactionConfig & FunctionName,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(dsname, func, config);
  }

  // Custom TX decorator
  transaction(config?: KnexTransactionConfig) {
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

      descriptor.value = ds.registerTransaction(descriptor.value, config);

      return descriptor;
    };
  }

  async runTransaction<T>(callback: () => Promise<T>, config?: KnexTransactionConfig) {
    return await runTransaction(callback, config?.name ?? callback.name, { dsName: this.name, config });
  }
}

////
/// App logic to test
////

const config = generateDBOSTestConfig();

async function txFunctionGuts() {
  expect(DBOS.isInTransaction()).toBe(true);
  expect(DBOS.isWithinWorkflow()).toBe(true);
  const res = await DBOSKnexDS.knexClient.raw<{ rows: { a: string }[] }>("SELECT 'Tx2 result' as a");
  return res.rows[0].a;
}

// It is not clear if we want to encourage this pattern of registering early by DS name, but it does work
const txFunc = DBOSKnexDS.registerTransaction('knexA', txFunctionGuts, { name: 'MySecondTx' });

async function wfFunctionGuts() {
  // Transaction variant 2: Let DBOS run a code snippet as a step
  const p1 = await dsa.runTransaction(
    async () => {
      return (await DBOSKnexDS.knexClient.raw<{ rows: { a: string }[] }>("SELECT 'My first tx result' as a")).rows[0].a;
    },
    { name: 'MyFirstTx', readOnly: true },
  );

  // Transaction variant 1: Use a registered DBOS transaction function
  const p2 = await txFunc();

  return p1 + '|' + p2;
}

// Workflow functions must always be registered before launch; this
//  allows recovery to occur.
const wfFunction = DBOS.registerWorkflow(wfFunctionGuts, { name: 'workflow' });

// Intentionally initialize DS after we've already tried to register a transaction to it
const dsa = new DBOSKnexDS('knexA', { connectionString: config.systemDatabaseUrl });

// Decoratory example
class DBWFI {
  @dsa.transaction({ readOnly: true })
  static async tx() {
    return (await DBOSKnexDS.knexClient.raw<{ rows: { a: string }[] }>("SELECT 'My decorated tx result' as a")).rows[0]
      .a;
  }

  @DBOS.workflow()
  static async wf() {
    return await DBWFI.tx();
  }

  @DBOS.requiredRole(['user'])
  @dsa.transaction({ readOnly: true })
  static async sectx1() {
    return (await DBOSKnexDS.knexClient.raw<{ rows: { a: string }[] }>("SELECT 'Secure Tx1' as a")).rows[0].a;
  }

  @dsa.transaction({ readOnly: true })
  @DBOS.requiredRole(['user'])
  static async sectx2() {
    return (await DBOSKnexDS.knexClient.raw<{ rows: { a: string }[] }>("SELECT 'Secure Tx1' as a")).rows[0].a;
  }

  @DBOS.workflow()
  static async wfs1() {
    return await DBWFI.sectx1();
  }

  @DBOS.workflow()
  static async wfs2() {
    return await DBWFI.sectx2();
  }
}

async function txFunctionGutsNoWF() {
  expect(DBOS.isInTransaction()).toBe(true);
  expect(DBOS.isWithinWorkflow()).toBe(false);
  const res = await DBOSKnexDS.knexClient.raw<{ rows: { a: string }[] }>("SELECT 'NoWF Tx Result' as a");
  return res.rows[0].a;
}

const txFuncNoWF = dsa.registerTransaction(txFunctionGutsNoWF, {});

describe('decoratorless-api-tests', () => {
  beforeAll(async () => {
    await setUpDBOSTestSysDb(config);
    await dsa.initializeDBOSSchema();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('bare-tx-wf-functions', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await wfFunction();
      expect(res).toBe('My first tx result|Tx2 result');
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('MyFirstTx');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].name).toBe('MySecondTx');

    // Check that the bare transaction does not start a workflow
    const nwsBefore = (await DBOS.listWorkflows({})).length;
    const p1 = await dsa.runTransaction(
      async () => {
        return (await DBOSKnexDS.knexClient.raw<{ rows: { a: string }[] }>("SELECT 'Bare outside wf' as a")).rows[0].a;
      },
      { name: 'MyFirstTx', readOnly: true },
    );
    expect(p1).toBe('Bare outside wf');

    const res = await txFuncNoWF();
    expect(res).toBe('NoWF Tx Result');

    const nwsAfter = (await DBOS.listWorkflows({})).length;
    expect(nwsAfter - nwsBefore).toBe(0);
  });

  test('decorated-tx-wf-functions', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await DBWFI.wf();
      expect(res).toBe('My decorated tx result');
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('tx');

    // Check that the bare transaction does not start a workflow
    const nwsBefore = (await DBOS.listWorkflows({})).length;
    expect(nwsBefore).toBeGreaterThanOrEqual(1);
    const res = await DBWFI.tx();
    expect(res).toBe('My decorated tx result');
    const nwsAfter = (await DBOS.listWorkflows({})).length;
    expect(nwsAfter - nwsBefore).toBe(0);

    //  (If WF requested by providing an ID, this is an error)
    await DBOS.withNextWorkflowID(wfid, async () => {
      await expect(DBWFI.tx()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
    });
  });

  test('security-plus-dstxns', async () => {
    await expect(DBWFI.sectx1()).rejects.toThrow(DBOSNotAuthorizedError);
    await expect(DBWFI.sectx2()).rejects.toThrow(DBOSNotAuthorizedError);
    await expect(DBWFI.wfs1()).rejects.toThrow(DBOSNotAuthorizedError);
    await expect(DBWFI.wfs2()).rejects.toThrow(DBOSNotAuthorizedError);
  });
});

////
/// Duplicate execution: replaying a step the winner already recorded
////

const WINNER_TX_OUTPUT = 'winner-tx-output';
const ADOPTED_WF_OUTPUT = 'adopted-workflow-output';

// A duplicate execution's step checkpoint must look older than ours, or the
// system database's same-millisecond comparison would not see a conflict.
const winnerEpochMs = () => Date.now() - 60_000;

type CompletionRow = { output: string | null; error: string | null };

/**
 * The smallest data source that follows the current contract: detect a lost
 * completion insert with ON CONFLICT DO NOTHING, then replay the winner.
 */
class ProbeTransactionHandler implements DataSourceTransactionHandler {
  readonly name = 'probe-ds';
  readonly dsType = 'ProbeDataSource';
  #poolField: Pool | undefined;

  async initialize(): Promise<void> {
    this.#poolField = new Pool({ connectionString: config.systemDatabaseUrl });
    await this.#poolField.query(createTransactionCompletionSchemaPG());
    await this.#poolField.query(createTransactionCompletionTablePG());
  }

  async destroy(): Promise<void> {
    const pool = this.#poolField;
    this.#poolField = undefined;
    await pool?.end();
  }

  get pool(): Pool {
    if (!this.#poolField) {
      throw new Error('ProbeTransactionHandler is not initialized');
    }
    return this.#poolField;
  }

  // Returns the raw row, exercising replayRecordedStep's tolerance of both columns.
  async #checkExecution(workflowID: string, stepID: number): Promise<CompletionRow | undefined> {
    const { rows } = await this.pool.query<CompletionRow>(
      `SELECT output, error FROM dbos.transaction_completion WHERE workflow_id = $1 AND function_num = $2`,
      [workflowID, stepID],
    );
    return rows[0];
  }

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    _config: unknown,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const workflowID = DBOS.workflowID!;
    const stepID = DBOS.stepID!;

    const previous = await this.#checkExecution(workflowID, stepID);
    if (previous) {
      return replayRecordedStep<Return>(previous);
    }

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await func.call(target, ...args);
      const { rows } = await client.query(
        `INSERT INTO dbos.transaction_completion (workflow_id, function_num, output)
         VALUES ($1, $2, $3)
         ON CONFLICT (workflow_id, function_num) DO NOTHING
         RETURNING workflow_id`,
        [workflowID, stepID, SuperJSON.stringify(result)],
      );
      if (rows.length === 0) {
        throw new DBOSStepAlreadyRecordedError(workflowID, stepID);
      }
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      if (error instanceof DBOSStepAlreadyRecordedError) {
        const recorded = await this.#checkExecution(workflowID, stepID);
        if (recorded === undefined) {
          throw new DBOSError(`Step ${stepID} of workflow ${workflowID} conflicted but nothing was recorded`);
        }
        return replayRecordedStep<Return>(recorded);
      }
      throw error;
    } finally {
      client.release();
    }
  }
}

const probeHandler = new ProbeTransactionHandler();
registerDataSource(probeHandler);

const probeState = { bodyRuns: 0, workflowBodyFinished: 0, claimSysdb: false, winnerRecordedOutput: false };

/**
 * Mid-transaction, a duplicate execution commits the app-database completion row.
 * When `claimSysdb` is set it also takes the system-database step checkpoint and
 * finishes the workflow, which is what forces this run to park instead of continuing.
 */
async function raceTransaction(): Promise<string> {
  probeState.bodyRuns += 1;
  const workflowID = DBOS.workflowID!;
  const stepID = DBOS.stepID!;

  const winner = new Client({ connectionString: config.systemDatabaseUrl });
  try {
    await winner.connect();
    await winner.query(
      `INSERT INTO dbos.transaction_completion (workflow_id, function_num, output) VALUES ($1, $2, $3)`,
      [workflowID, stepID, SuperJSON.stringify(WINNER_TX_OUTPUT)],
    );
  } finally {
    await winner.end();
  }

  if (probeState.claimSysdb) {
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const winnerMs = winnerEpochMs();
    await sysdb.recordOperationResult(workflowID, stepID, 'raceTransaction', false, winnerMs, winnerMs, {
      output: DBOSJSON.stringify(WINNER_TX_OUTPUT),
    });
    probeState.winnerRecordedOutput = await sysdb.recordWorkflowOutput(workflowID, {
      output: DBOSJSON.stringify(ADOPTED_WF_OUTPUT),
    } as WorkflowStatusInternal);
  }

  return 'loser-tx-output';
}

const regRaceTransaction = registerTransaction(probeHandler.name, raceTransaction, { name: 'raceTransaction' });

async function raceWorkflowGuts(): Promise<string> {
  // Prefixed so a locally computed result can never be mistaken for the adopted one.
  const tx = await regRaceTransaction();
  // Only reached when the step returned; a parked run throws out of the line above.
  probeState.workflowBodyFinished += 1;
  return `local:${tx}`;
}

const raceWorkflow = DBOS.registerWorkflow(raceWorkflowGuts, { name: 'raceWorkflow' });

describe('datasource-duplicate-execution', () => {
  beforeAll(async () => {
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    probeState.bodyRuns = 0;
    probeState.workflowBodyFinished = 0;
    probeState.claimSysdb = false;
    probeState.winnerRecordedOutput = false;
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('a losing duplicate execution replays the winner transaction and carries on', async () => {
    const wfid = randomUUID();

    // The winner committed only the app-database row, which is the crash window this
    // whole mechanism exists for: the completion row is durable, the checkpoint is not.
    const result = await DBOS.withNextWorkflowID(wfid, () => raceWorkflow());

    expect(result).toBe(`local:${WINNER_TX_OUTPUT}`); // the winner's value, not this run's
    expect(probeState.bodyRuns).toBe(1); // the replay did not re-enter the transaction body
    expect(probeState.workflowBodyFinished).toBe(1);

    const steps = await DBOS.listWorkflowSteps(wfid);
    expect(steps).toHaveLength(1);
    expect(steps![0].output).toBe(WINNER_TX_OUTPUT);
    expect((await DBOS.getWorkflowStatus(wfid))?.status).toBe('SUCCESS');
  });

  test('a losing duplicate execution parks when the winner also owns the checkpoint', async () => {
    const wfid = randomUUID();
    probeState.claimSysdb = true;

    const result = await DBOS.withNextWorkflowID(wfid, () => raceWorkflow());

    // The step conflict aborted this run mid-workflow, so its body never finished and
    // the recorded outcome was adopted in place of the `local:` value it would have made.
    expect(probeState.winnerRecordedOutput).toBe(true);
    expect(probeState.workflowBodyFinished).toBe(0);
    expect(result).toBe(ADOPTED_WF_OUTPUT);
    expect(probeState.bodyRuns).toBe(1);

    // The winner's records still stand, and the loser wrote nothing over them.
    const { rows: completions } = await probeHandler.pool.query<CompletionRow>(
      `SELECT output, error FROM dbos.transaction_completion WHERE workflow_id = $1`,
      [wfid],
    );
    expect(completions).toHaveLength(1);
    expect(completions[0].error).toBeNull();
    expect(SuperJSON.parse(completions[0].output!)).toBe(WINNER_TX_OUTPUT);

    const steps = await DBOS.listWorkflowSteps(wfid);
    expect(steps).toHaveLength(1);
    expect(steps![0].output).toBe(WINNER_TX_OUTPUT);
    expect((await DBOS.getWorkflowStatus(wfid))?.status).toBe('SUCCESS');
  });
});
