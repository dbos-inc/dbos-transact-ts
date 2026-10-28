// What core uses from @dbos-inc/dbos-enterprise, which checks it satisfies these types; loaded only when needed.
// Also the entitlement check every enterprise feature goes through before the package is loaded.
//
// Transact Enterprise (the @dbos-inc/dbos-enterprise package) is proprietary, not MIT-licensed like this
// project. Its license (https://www.dbos.dev/conductor-license) forbids modifying it, and makes disabling
// or circumventing the entitlement checks here a material breach.

import type { Pool } from 'pg';
import type { DBOSExecutor } from './dbos-executor';
import { DBOSInitializationError } from './error';
import type { DBOSSerializer } from './serialization';
import type { SystemDatabase } from './system_database';
import { GlobalLogger } from './telemetry/logs';
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

/** One tick of auto-upgrade. */
export interface AutoUpgradePass {
  /** Move work left on older application versions to the latest one, stopping at a chunk boundary once `signal` aborts. Returns how many workflows moved. */
  run(signal: AbortSignal): Promise<number>;
  close(): Promise<void>;
}

export interface AutoUpgradeOptions {
  /** How long the latest version must have been the latest before PENDING workflows on older versions are re-enqueued. */
  graceMs: number;
}

export type AutoUpgradePassFactory = new (executor: DBOSExecutor, options: AutoUpgradeOptions) => AutoUpgradePass;

/** What a sharded system database is built from, in place of core's Postgres one. */
export interface ShardedSystemDatabaseOptions {
  systemDatabaseUrl: string;
  logger: GlobalLogger;
  serializer: DBOSSerializer;
  sysDbPoolSize?: number;
  systemDatabasePool?: Pool;
  schemaName?: string;
  pollingConcurrency?: number;
  appName?: string;
  observabilityQueryTimeoutMs?: number;
  idleTransactionTimeoutMs?: number;
}

export type ShardedSystemDatabaseFactory = new (options: ShardedSystemDatabaseOptions) => SystemDatabase;

/**
 * Suspend mode for recv and sleep: a waiting recv or a sleep parks its workflow in the database as
 * SUSPENDED instead of holding it in memory; a send on the recv's topic, the recv timeout, or the end
 * of the sleep re-enqueues it. The send side is core's (SystemDatabase.wakeReceivers), so any sender
 * wakes a parked recv.
 */
export interface Suspend {
  /**
   * The recorded or waiting message, or null once the timeout has passed. Otherwise parks the
   * workflow and throws DBOSWorkflowSuspendedError, so the execution exits.
   */
  recvSuspend(
    workflowID: string,
    functionID: number,
    timeoutFunctionID: number,
    topic: string | undefined,
    timeoutSeconds: number | undefined,
  ): Promise<{ serializedValue: string | null; serialization: string | null }>;
  /**
   * Checkpoint the sleep and park the workflow until its end time, throwing DBOSWorkflowSuspendedError;
   * or return the milliseconds to sleep in place when that end time is near or past.
   */
  sleepSuspend(workflowID: string, functionID: number, durationMS: number): Promise<number>;
  /** Re-enqueue the SUSPENDED workflows whose wake time has come. */
  transitionSuspendedWorkflows(): Promise<void>;
}

export type SuspendFactory = new (sysdb: SystemDatabase) => Suspend;

/** What the @dbos-inc/dbos-enterprise module must export. */
export interface Enterprise {
  ConductorWebsocket: ConductorFactory;
  AutoUpgradePass: AutoUpgradePassFactory;
  ShardedSystemDatabase: ShardedSystemDatabaseFactory;
  Suspend: SuspendFactory;
}

export const CONDUCTOR = 'Connecting to DBOS Conductor';

/** A Transact Enterprise feature. */
export interface Feature {
  readonly entitlement: string;
  readonly description: string;
  readonly configKey?: 'autoUpgrade' | 'sharding' | 'suspendMode';
}

export const AUTO_UPGRADE: Feature = {
  entitlement: 'autoUpgrade',
  description: 'Auto-upgrading workflows',
  configKey: 'autoUpgrade',
};

export const SHARDING: Feature = {
  entitlement: 'sharding',
  description: 'Using a sharded system database',
  configKey: 'sharding',
};

export const SUSPEND: Feature = {
  entitlement: 'suspendWorkflows',
  description: 'Suspending workflows on recv and sleep',
  configKey: 'suspendMode',
};

// Every entitled feature; a new one is a Feature here, named by Conductor's camelCase key.
export const FEATURES: readonly Feature[] = [AUTO_UPGRADE, SHARDING, SUSPEND];

/** The enterprise features a config turns on. */
export function enabledFeatures(config: {
  autoUpgrade?: boolean;
  sharding?: boolean;
  suspendMode?: boolean;
}): Feature[] {
  return FEATURES.filter((f) => f.configKey !== undefined && config[f.configKey] === true);
}

export const ENTITLEMENTS_PATH = '/v2/entitlements';
export const ENTITLEMENT_TIMEOUT_MS = 10_000;
const ERROR_CODE_HEADER = 'X-DBOS-Error';
const PAST_LIMIT = 'past_limit';
// From the hosted Conductor, these mean it could not answer, not that it refused the key.
const HOSTED_UNAVAILABLE = new Set([403, 407, 408, 425, 429]);
const LICENSE_URL = 'https://www.dbos.dev/conductor-license';
const LICENSE_NOTICE =
  `Transact Enterprise is licensed under the DBOS Conductor Software License (${LICENSE_URL}), ` +
  'which makes disabling or circumventing this check a material breach.';

function refusal(message: string): DBOSInitializationError {
  return new DBOSInitializationError(`${message} ${LICENSE_NOTICE}`);
}

export const HOSTED_CONDUCTOR_URL = 'wss://cloud.dbos.dev/conductor/v1alpha1';

export function defaultConductorURL(): string {
  return `wss://${process.env.DBOS_DOMAIN || 'cloud.dbos.dev'}/conductor/v1alpha1`;
}

/** The entitlement route of the Conductor at `conductorURL`, over HTTP(S). */
export function entitlementURL(conductorURL: string): string {
  const url = new URL(conductorURL);
  url.protocol = { 'wss:': 'https:', 'ws:': 'http:' }[url.protocol] ?? url.protocol;
  // The hosted websocket URL carries an API version; routes sit beside it.
  let path = url.pathname.replace(/\/+$/, '');
  if (path.endsWith('/v1alpha1')) {
    path = path.slice(0, -'/v1alpha1'.length);
  }
  url.pathname = path + ENTITLEMENTS_PATH;
  // Clean up potential private credentials
  url.username = '';
  url.password = '';
  url.search = '';
  url.hash = '';
  return url.toString();
}

interface RequireEnterpriseOptions {
  enterpriseKey?: string;
  conductorKey?: string;
  conductorURL?: string;
}

function problemDetail(body: string): string | undefined {
  try {
    const parsed = JSON.parse(body) as unknown;
    if (parsed && typeof parsed === 'object') {
      const detail =
        (parsed as { detail?: unknown; message?: unknown }).detail ?? (parsed as { message?: unknown }).message;
      if (detail) return typeof detail === 'string' ? detail : JSON.stringify(detail);
    }
  } catch {
    // Not JSON: a bare router answer, such as "404 page not found".
  }
  return undefined;
}

function parseEntitlements(body: string): Record<string, boolean> {
  const parsed = JSON.parse(body) as unknown;
  const entitlements =
    parsed && typeof parsed === 'object' ? (parsed as { entitlements?: unknown }).entitlements : undefined;
  if (!entitlements || typeof entitlements !== 'object' || Array.isArray(entitlements)) {
    throw new Error('the response has no entitlements');
  }
  return Object.fromEntries(Object.entries(entitlements as Record<string, unknown>).map(([k, v]) => [k, v === true]));
}

/**
 * Enterprise features use Conductor to report the org entitled to each of them. A
 * self-hosted Conductor may answer without a key; DBOS Cloud passes without a check.
 *
 * A license control: the Transact Enterprise license makes disabling or circumventing it a material breach.
 */
export async function requireEnterprise(
  features: readonly Feature[],
  options: RequireEnterpriseOptions,
): Promise<void> {
  if (features.length === 0 || globalParams.dbosCloud) {
    return;
  }
  const names = features.map((f) => f.description).join(' and ');
  const entitlements = features.map((f) => f.entitlement).join(' and ');
  const key = options.enterpriseKey || options.conductorKey;
  const target = options.conductorURL || defaultConductorURL();
  const keyHint = 'Set transactEnterpriseKey in your DBOS configuration, or pass a conductorKey to DBOS.launch.';
  // Only a Conductor named explicitly is asked without a key, and never DBOS's hosted one.
  if (!key && (!options.conductorURL || target === HOSTED_CONDUCTOR_URL)) {
    throw new DBOSInitializationError(`${names} requires a DBOS key. ${keyHint}`);
  }
  const url = entitlementURL(target);
  const headers: Record<string, string> = { 'User-Agent': `dbos-transact-ts/${globalParams.dbosVersion}` };
  if (key) {
    headers.Authorization = `Bearer ${key}`;
  }

  let failure: string;
  try {
    let response: Response;
    let body: string;
    try {
      // Never follow a redirect: it would carry the key to whatever host it names.
      response = await fetch(url, { headers, redirect: 'manual', signal: AbortSignal.timeout(ENTITLEMENT_TIMEOUT_MS) });
      body = await response.text();
    } catch (e) {
      throw new EntitlementFailure(
        e instanceof Error ? (e.cause instanceof Error ? e.cause.message : e.message) : String(e),
      );
    }

    if (!response.ok) {
      const status = response.status;
      const detail = problemDetail(body);
      if (status === 403 && response.headers.get(ERROR_CODE_HEADER) === PAST_LIMIT) {
        throw refusal(
          `Your DBOS organization is past its plan limit, so it is not entitled to ${entitlements} (${names}). Upgrade its plan to continue.`,
        );
      }
      const reason = detail ?? response.statusText;
      if (target === HOSTED_CONDUCTOR_URL && HOSTED_UNAVAILABLE.has(status)) {
        // An outage, handled below.
      } else if (status === 404 && detail === undefined) {
        // A bare 404 is a missing route: a Conductor predating per-feature entitlements.
        throw refusal(
          `${names} requires an entitled DBOS key, but Conductor at ${url} has no entitlements route (HTTP 404). Upgrade Conductor, or check its URL.`,
        );
      } else if (status === 401 && !key) {
        throw new DBOSInitializationError(
          `${names} requires a DBOS key: Conductor at ${url} refused to check without one (HTTP 401): ${reason}. ${keyHint}`,
        );
      } else if (status === 401) {
        // An unknown key, or a user credential instead of an API key.
        throw refusal(`${names} requires a valid DBOS key. Conductor at ${url} rejected the key (HTTP 401): ${reason}`);
      } else if (status < 500) {
        // Any other refusal, such as the key's organization being gone, or a redirect.
        throw refusal(
          `${names} requires an entitled DBOS key, but Conductor at ${url} refused the check (HTTP ${status}): ${reason}`,
        );
      }
      throw new EntitlementFailure(detail ? `HTTP ${status}: ${detail}` : `HTTP ${status}`);
    }

    let granted: Record<string, boolean>;
    try {
      granted = parseEntitlements(body);
    } catch (e) {
      throw new EntitlementFailure((e as Error).message);
    }
    const missing = features.filter((f) => granted[f.entitlement] !== true);
    if (missing.length > 0) {
      throw refusal(
        `Your DBOS organization is not entitled to ${missing.map((f) => f.entitlement).join(' and ')} ` +
          `(${missing.map((f) => f.description).join(' and ')}): its plan does not include it. Contact DBOS to enable it.`,
      );
    }
    return;
  } catch (e) {
    if (!(e instanceof EntitlementFailure)) throw e;
    failure = e.message;
  }
  if (target === HOSTED_CONDUCTOR_URL) {
    new GlobalLogger().warn(
      `Could not verify the DBOS key's entitlements with Conductor at ${url} (${failure}); starting anyway.`,
    );
    return;
  }
  // Any other Conductor fails closed, so an unreachable URL cannot stand in for a key.
  throw refusal(
    `${names} requires an entitled DBOS key, but checking it with Conductor at ${url} failed (${failure}). Retry once Conductor is reachable.`,
  );
}

/** A check that got no answer from Conductor, as opposed to a refusal. */
class EntitlementFailure extends Error {}

function unavailable(feature: string, cause: string): DBOSInitializationError {
  return new DBOSInitializationError(
    `${feature} requires ${ENTERPRISE_PACKAGE}, at the same minor version as @dbos-inc/dbos-sdk ` +
      `(${globalParams.dbosVersion}). Install or upgrade it with \`npm install ${ENTERPRISE_PACKAGE}\`. Cause: ${cause}`,
  );
}

/** Load @dbos-inc/dbos-enterprise, for `feature`, which the error names when it cannot be loaded. */
export function load(feature: string = CONDUCTOR): Enterprise {
  // @dbos-inc/dbos-enterprise is proprietary: its license forbids modifying it or copying it beyond authorized use.
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
    throw unavailable(feature, e instanceof Error ? e.message.split('\n')[0] : String(e));
  }
  for (const name of ['ConductorWebsocket', 'AutoUpgradePass', 'ShardedSystemDatabase', 'Suspend'] as const) {
    if (typeof loaded?.[name] !== 'function') {
      throw unavailable(feature, `it does not export ${name}`);
    }
  }
  return loaded as Enterprise;
}
