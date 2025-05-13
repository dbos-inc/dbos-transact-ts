import { MethodRegistration } from './decorators';

// Data source implementation (to be moved to DBOS core)
export interface DBOSTransactionalDataSource {
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
