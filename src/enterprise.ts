// What core uses from @dbos-inc/dbos-enterprise, which checks it satisfies these types; loaded only when needed.

import type { DBOSExecutor } from './dbos-executor';
import { DBOSInitializationError } from './error';
import { globalParams } from './utils';

export const ENTERPRISE_PACKAGE = '@dbos-inc/dbos-enterprise';

// Tests substitute the loaded module here.
export const enterpriseLoader = {
  override: undefined as (() => unknown) | undefined,
};

/** The Conductor client, connected for the life of a launched DBOS instance. */
export interface ConductorConnection {
  start(): void;
  /** Disconnect, giving a retention round in flight a bounded chance to finish. */
  stop(): Promise<void>;
}

export interface ConductorOptions {
  appName: string;
  conductorURL: string;
  conductorKey: string;
  executorMetadata?: Record<string, unknown>;
  metadataOnlyMode?: boolean;
}

export type ConductorFactory = new (executor: DBOSExecutor, options: ConductorOptions) => ConductorConnection;

/** What the @dbos-inc/dbos-enterprise module must export. */
export interface Enterprise {
  ConductorWebsocket: ConductorFactory;
}

// One message for every failure: bundlers report an absent package too many different ways to tell apart reliably.
function unavailable(cause: string): DBOSInitializationError {
  return new DBOSInitializationError(
    `Connecting to DBOS Conductor requires ${ENTERPRISE_PACKAGE}, at the same minor version as @dbos-inc/dbos-sdk ` +
      `(${globalParams.dbosVersion}). Install or upgrade it with \`npm install ${ENTERPRISE_PACKAGE}\`. Cause: ${cause}`,
  );
}

/** Load @dbos-inc/dbos-enterprise. */
export function load(): Enterprise {
  let loaded: Partial<Enterprise> | undefined;
  try {
    // A literal specifier inside try: bundlers include the package when it is installed and tolerate its absence.
    loaded = (
      enterpriseLoader.override
        ? enterpriseLoader.override()
        : // eslint-disable-next-line @typescript-eslint/no-require-imports
          require('@dbos-inc/dbos-enterprise')
    ) as Partial<Enterprise> | undefined;
  } catch (e) {
    throw unavailable(e instanceof Error ? e.message.split('\n')[0] : String(e));
  }
  if (typeof loaded?.ConductorWebsocket !== 'function') {
    throw unavailable('it does not export ConductorWebsocket');
  }
  return loaded as Enterprise;
}
