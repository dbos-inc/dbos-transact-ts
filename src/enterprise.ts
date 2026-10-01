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

// Node reports a missing package by the specifier that failed, so a missing dependency of enterprise differs.
function isEnterpriseMissing(e: unknown): boolean {
  const err = e as { code?: string; message?: string };
  return err?.code === 'MODULE_NOT_FOUND' && (err.message ?? '').split('\n')[0].includes(`'${ENTERPRISE_PACKAGE}'`);
}

function versionMismatch(cause: string): DBOSInitializationError {
  // The package imports core internals, so failing to load is almost always a version mismatch.
  return new DBOSInitializationError(
    `The ${ENTERPRISE_PACKAGE} package is installed but could not be loaded by @dbos-inc/dbos-sdk ${globalParams.dbosVersion}. ` +
      `The two must share a minor version; upgrade both together. Cause: ${cause}`,
  );
}

/** Load @dbos-inc/dbos-enterprise, telling an absent package apart from one that fails to load. */
export function load(): Enterprise {
  let loaded: Partial<Enterprise>;
  try {
    // A literal specifier inside try: bundlers include the package when it is installed and tolerate its absence.
    loaded = (
      enterpriseLoader.override
        ? enterpriseLoader.override()
        : // eslint-disable-next-line @typescript-eslint/no-require-imports
          require('@dbos-inc/dbos-enterprise')
    ) as Partial<Enterprise>;
  } catch (e) {
    if (isEnterpriseMissing(e)) {
      throw new DBOSInitializationError(
        `Connecting to DBOS Conductor requires the ${ENTERPRISE_PACKAGE} package. Install it with \`npm install ${ENTERPRISE_PACKAGE}\`.`,
        e as Error,
      );
    }
    throw versionMismatch((e as Error).message?.split('\n')[0] ?? String(e));
  }
  if (typeof loaded?.ConductorWebsocket !== 'function') {
    throw versionMismatch('it does not export ConductorWebsocket');
  }
  return loaded as Enterprise;
}
