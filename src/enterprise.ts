// What core uses from @dbos-inc/dbos-enterprise, which checks it satisfies these types; loaded only when needed.
// Also the entitlement check every enterprise feature goes through before the package is loaded.

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

/** What the @dbos-inc/dbos-enterprise module must export. */
export interface Enterprise {
  ConductorWebsocket: ConductorFactory;
  AutoUpgradePass: AutoUpgradePassFactory;
}

export const CONDUCTOR = 'Connecting to DBOS Conductor';

/** A Transact Enterprise feature. */
export interface Feature {
  readonly entitlement: string;
  readonly description: string;
  readonly configKey?: 'autoUpgrade';
}

export const AUTO_UPGRADE: Feature = {
  entitlement: 'autoUpgrade',
  description: 'Auto-upgrading workflows',
  configKey: 'autoUpgrade',
};

export const FEATURES: readonly Feature[] = [AUTO_UPGRADE];

/** The enterprise features a config turns on. */
export function enabledFeatures(config: { autoUpgrade?: boolean }): Feature[] {
  return FEATURES.filter((f) => f.configKey !== undefined && config[f.configKey] === true);
}

export const ENTITLEMENTS_PATH = '/v2/entitlements';
export const ENTITLEMENT_TIMEOUT_MS = 10_000;
const ERROR_CODE_HEADER = 'X-DBOS-Error';
const PAST_LIMIT = 'past_limit';

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
 * Any failure to get an answer refuses to start: an unreachable Conductor cannot stand
 * in for an entitlement.
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

  let response: Response;
  let body: string;
  try {
    // Never follow a redirect: it would carry the key to whatever host it names.
    response = await fetch(url, { headers, redirect: 'manual', signal: AbortSignal.timeout(ENTITLEMENT_TIMEOUT_MS) });
    body = await response.text();
  } catch (e) {
    // Fail closed, so an unreachable URL cannot stand in for a key.
    const cause = e instanceof Error ? (e.cause instanceof Error ? e.cause.message : e.message) : String(e);
    throw new DBOSInitializationError(
      `${names} requires an entitled DBOS key, but checking it with Conductor at ${url} failed (${cause}). Please retry later.`,
    );
  }

  if (!response.ok) {
    const status = response.status;
    const detail = problemDetail(body);
    if (status === 403 && response.headers.get(ERROR_CODE_HEADER) === PAST_LIMIT) {
      throw new DBOSInitializationError(
        `Your DBOS organization is past its plan limit, so it is not entitled to ${entitlements} (${names}). Upgrade its plan to continue.`,
      );
    }
    const reason = detail ?? response.statusText;
    if (status === 404 && detail === undefined) {
      // A bare 404 is a missing route: a Conductor predating per-feature entitlements.
      throw new DBOSInitializationError(
        `${names} requires an entitled DBOS key, but Conductor at ${url} has no entitlements route (HTTP 404). Upgrade Conductor, or check its URL.`,
      );
    }
    if (status === 401 && !key) {
      throw new DBOSInitializationError(
        `${names} requires a DBOS key: Conductor at ${url} refused to check without one (HTTP 401): ${reason}. ${keyHint}`,
      );
    }
    if (status === 401) {
      // An unknown key, or a user credential instead of an API key.
      throw new DBOSInitializationError(
        `${names} requires a valid DBOS key. Conductor at ${url} rejected the key (HTTP 401): ${reason}`,
      );
    }
    if (status < 500) {
      // Any other refusal, such as the key's organization being gone, or a redirect.
      throw new DBOSInitializationError(
        `${names} requires an entitled DBOS key, but Conductor at ${url} refused the check (HTTP ${status}): ${reason}`,
      );
    }
    const failure = detail ? `HTTP ${status}: ${detail}` : `HTTP ${status}`;
    throw new DBOSInitializationError(
      `${names} requires an entitled DBOS key, but checking it with Conductor at ${url} failed (${failure}). Please retry later.`,
    );
  }

  let granted: Record<string, boolean>;
  try {
    granted = parseEntitlements(body);
  } catch (e) {
    throw new DBOSInitializationError(
      `${names} requires an entitled DBOS key, but unable to process the entitlements answer from ${url} (${(e as Error).message}).`,
    );
  }
  const missing = features.filter((f) => granted[f.entitlement] !== true);
  if (missing.length > 0) {
    throw new DBOSInitializationError(
      `Your DBOS organization is not entitled to ${missing.map((f) => f.entitlement).join(' and ')} ` +
        `(${missing.map((f) => f.description).join(' and ')}): its plan does not include it. Contact DBOS to enable it.`,
    );
  }
}

function unavailable(feature: string, cause: string): DBOSInitializationError {
  return new DBOSInitializationError(
    `${feature} requires ${ENTERPRISE_PACKAGE}, at the same minor version as @dbos-inc/dbos-sdk ` +
      `(${globalParams.dbosVersion}). Install or upgrade it with \`npm install ${ENTERPRISE_PACKAGE}\`. Cause: ${cause}`,
  );
}

/** Load @dbos-inc/dbos-enterprise, for `feature`, which the error names when it cannot be loaded. */
export function load(feature: string = CONDUCTOR): Enterprise {
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
  for (const name of ['ConductorWebsocket', 'AutoUpgradePass'] as const) {
    if (typeof loaded?.[name] !== 'function') {
      throw unavailable(feature, `it does not export ${name}`);
    }
  }
  return loaded as Enterprise;
}
