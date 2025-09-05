import type { GeneratedMigration } from './migration_types';

import type { Client } from 'pg';

/** Get the current DB version, or 0 if table is missing/empty. */
export async function getCurrentSysDBVersion(client: Client): Promise<number> {
  // Does table exist?
  const regRes = await client.query<{ table_name: string | null }>(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'dbos_migrations'",
    [],
  );
  if (!regRes.rows[0]?.table_name) return 0;

  const verRes = await client.query<{ version: string | number }>(
    `select version from "dbos"."dbos_migrations" order by version desc limit 1`,
  );
  if (verRes.rowCount === 0) return 0;

  const raw = verRes.rows[0].version;
  const n = typeof raw === 'string' ? Number(raw) : Number(raw);
  return Number.isFinite(n) ? n : 0;
}

export type PgMigratorOptions = {
  ignoreErrorCodes?: ReadonlySet<string>;
  onWarn?: (msg: string, err?: unknown) => void;
};

const DEFAULT_IGNORABLE_CODES = new Set<string>([
  // Relation / object already exists
  '42P07', // duplicate_table
  '42710', // duplicate_object (e.g., index)
  '42701', // duplicate_column
  '42P06', // duplicate_schema
  // Uniqueness (e.g., insert seed rows twice)
  '23505', // unique_violation
]);

type PgErrorLike = { code?: string; message?: string };
function isPgErrorLike(x: unknown): x is PgErrorLike {
  return typeof x === 'object' && x !== null && ('code' in x || 'message' in x);
}

function isDDLAlreadyAppliedPgError(err: unknown, ignoreCodes: ReadonlySet<string>): boolean {
  if (!isPgErrorLike(err)) return false;
  if (err.code && ignoreCodes.has(err.code)) return true;
  const msg = err.message ?? '';
  // Fallback on message matching (best-effort)
  return /already exists/i.test(msg) || /duplicate/i.test(msg);
}

/** Run a list of statements, ignoring “already applied” errors. */
async function runStatementsIgnoring(
  client: Client,
  stmts: ReadonlyArray<string>,
  ignoreCodes: ReadonlySet<string>,
  warn: (m: string, e?: unknown) => void,
): Promise<void> {
  for (const s of stmts) {
    try {
      await client.query(s, []);
    } catch (err) {
      if (isDDLAlreadyAppliedPgError(err, ignoreCodes)) {
        warn(`Ignoring idempotent error while executing: ${s}`, err);
        continue;
      }
      throw err;
    }
  }
}

/**
 * Apply all migrations greater than the current DB version.
 * - Reads current version (0 if table missing/empty)
 * - Applies migrations in order
 * - Updates dbos_migrations.version at the end
 * - Warns if current version > max known (likely newer software concurrently)
 */
export async function runSysMigrationsPg(
  client: Client,
  allMigrations: ReadonlyArray<GeneratedMigration>,
  opts: PgMigratorOptions = {},
): Promise<{
  fromVersion: number;
  toVersion: number;
  appliedCount: number;
  skippedCount: number;
  notice?: string;
}> {
  const { ignoreErrorCodes = DEFAULT_IGNORABLE_CODES, onWarn = (m) => console.warn(m) } = opts;

  const current = await getCurrentSysDBVersion(client);
  const maxKnown = allMigrations.length;

  if (current > maxKnown) {
    return {
      fromVersion: current,
      toVersion: current,
      appliedCount: 0,
      skippedCount: allMigrations.length,
      notice:
        `Database version (${current}) is ahead of this build's max (${maxKnown}). ` +
        `A newer software version may be running concurrently.`,
    };
  }

  let applied = 0;
  let skipped = 0;
  let lastAppliedVersion = current;

  // Apply needed migrations in order
  for (let i = 0; i < allMigrations.length; i++) {
    const m = allMigrations[i];
    const v = i + 1;
    if (v <= current) {
      skipped++;
      continue;
    }

    const stmts = m.up.pg ?? [];
    if (stmts.length === 0) {
      onWarn(`Migration "${m.name}" has no Postgres statements; skipping.`);
      skipped++;
      continue;
    }

    await runStatementsIgnoring(client, stmts, ignoreErrorCodes, (msg, e) =>
      onWarn(`${msg}${e ? `\n  → ${String((e as { message?: string }).message ?? '')}` : ''}`),
    );
    applied++;
    lastAppliedVersion = v;
  }

  // Update version at the end (insert or update)
  const updateRes = await client.query(`UPDATE "dbos"."dbos_migrations" SET "version" = $1`, [lastAppliedVersion]);
  if (updateRes.rowCount === 0) {
    await client.query(`INSERT into "dbos"."dbos_migrations" ("version") values ($1)`, [lastAppliedVersion]);
  }

  return {
    fromVersion: current,
    toVersion: lastAppliedVersion,
    appliedCount: applied,
    skippedCount: skipped,
    notice:
      current < maxKnown && applied === 0 && skipped > 0
        ? 'Nothing to apply; DB is already up-to-date relative to known migrations.'
        : undefined,
  };
}
