/* eslint-disable @typescript-eslint/no-explicit-any */
import { UserDatabaseName, UserDatabaseClient } from './user_database';
import { WorkflowContextImpl } from './workflow';
import { Span } from '@opentelemetry/sdk-trace-base';
import { DBOSContext, DBOSContextImpl } from './context';
import { ValuesOf } from './utils';
import { GlobalLogger as Logger } from './telemetry/logs';

// Can we gradually call it TransactionFunction?
export type Transaction<T extends unknown[], R> = (ctxt: TransactionContext<any>, ...args: T) => Promise<R>;
export type TransactionFunction<T extends unknown[], R> = Transaction<T, R>;

export interface TransactionConfig {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
}

export const IsolationLevel = {
  ReadUncommitted: 'READ UNCOMMITTED',
  ReadCommitted: 'READ COMMITTED',
  RepeatableRead: 'REPEATABLE READ',
  Serializable: 'SERIALIZABLE',
} as const;
export type IsolationLevel = ValuesOf<typeof IsolationLevel>;

/**
 * @deprecated This class is no longer necessary
 * To update to Transact 2.0+
 *   Remove `TransactionContext` from function parameter lists
 *   Use `DBOS.` to access DBOS context within affected functions
 *   Adjust callers to call the function directly
 */
export interface TransactionContext<T extends UserDatabaseClient> extends DBOSContext {
  readonly client: T;
}

export class TransactionContextImpl<T extends UserDatabaseClient>
  extends DBOSContextImpl
  implements TransactionContext<T>
{
  constructor(
    readonly clientKind: UserDatabaseName,
    readonly client: T,
    workflowContext: WorkflowContextImpl,
    span: Span,
    logger: Logger,
    readonly functionID: number,
    operationName: string,
  ) {
    super(operationName, span, logger, workflowContext);
    this.applicationConfig = workflowContext.applicationConfig;
  }
}
