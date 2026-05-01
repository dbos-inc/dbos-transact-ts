import type { ClientBase } from 'pg';

export type DBMigration = {
  name?: string;
  pg?: ReadonlyArray<string>;
  sqlite3?: ReadonlyArray<string>;
  /**
   * If true, the migration is executed without wrapping it in any helper that
   * suppresses errors, and its statements must be safe to run outside a
   * transaction (e.g. `CREATE INDEX CONCURRENTLY`). The runner also cleans up
   * indexes left INVALID by a previously interrupted CONCURRENTLY build before
   * running the migration.
   */
  online?: boolean;
};

/** Get the current DB version, or 0 if table is missing/empty. */
export async function getCurrentSysDBVersion(client: ClientBase, schemaName: string = 'dbos'): Promise<number> {
  // Does table exist?
  const regRes = await client.query<{ table_name: string | null }>(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'dbos_migrations'",
    [schemaName],
  );
  if (!regRes.rows[0]?.table_name) return 0;

  const verRes = await client.query<{ version: string | number }>(
    `select version from "${schemaName}"."dbos_migrations" order by version desc limit 1`,
  );
  if (verRes.rowCount === 0) return 0;

  const raw = verRes.rows[0].version;
  const n = typeof raw === 'string' ? Number(raw) : Number(raw);
  return Number.isFinite(n) ? n : 0;
}

export type PgMigratorOptions = {
  ignoreErrorCodes?: ReadonlySet<string>;
  onWarn?: (msg: string, err?: unknown) => void;
  isCockroach?: boolean;
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
  return /already exists/i.test(msg) || /duplicate/i.test(msg) || /multiple.*?not allowed/i.test(msg);
}

/** Run a list of statements, ignoring “already applied” errors. */
async function runStatementsIgnoring(
  client: ClientBase,
  stmts: ReadonlyArray<string>,
  ignoreCodes: ReadonlySet<string>,
  warn: (m: string, e?: unknown) => void,
): Promise<void> {
  for (const s of stmts) {
    try {
      await client.query(s, []);
    } catch (err) {
      if (isDDLAlreadyAppliedPgError(err, ignoreCodes)) {
        warn(`Ignoring migration error; migration was likely already applied.  Occurred while executing: ${s}`, err);
        continue;
      }
      throw err;
    }
  }
}

/**
 * Drop indexes left INVALID by a prior interrupted `CREATE INDEX CONCURRENTLY`.
 * Postgres marks such indexes invalid; they continue to consume write overhead
 * without serving reads, so the next online migration cannot succeed by simply
 * re-running `IF NOT EXISTS` (the name is taken).
 */
async function cleanupInvalidIndexes(client: ClientBase, schemaName: string, warn: (m: string) => void): Promise<void> {
  const res = await client.query<{ relname: string }>(
    `SELECT i.relname
       FROM pg_index ix
       JOIN pg_class i ON i.oid = ix.indexrelid
       JOIN pg_class t ON t.oid = ix.indrelid
       JOIN pg_namespace n ON n.oid = t.relnamespace
      WHERE NOT ix.indisvalid AND n.nspname = $1`,
    [schemaName],
  );
  for (const row of res.rows) {
    warn(`Dropping invalid index "${schemaName}"."${row.relname}" left by a prior failed migration`);
    await client.query(`DROP INDEX CONCURRENTLY IF EXISTS "${schemaName}"."${row.relname}"`);
  }
}

async function bumpMigrationVersion(client: ClientBase, schemaName: string, version: number): Promise<void> {
  // The earliest migrations create the schema and the dbos_migrations table itself,
  // so the table is not available to update on the very first iterations of a fresh DB.
  const reg = await client.query<{ table_name: string | null }>(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'dbos_migrations'",
    [schemaName],
  );
  if (!reg.rows[0]?.table_name) return;

  const updateRes = await client.query(`UPDATE "${schemaName}"."dbos_migrations" SET "version" = $1`, [version]);
  if (updateRes.rowCount === 0) {
    await client.query(`INSERT INTO "${schemaName}"."dbos_migrations" ("version") VALUES ($1)`, [version]);
  }
}

/**
 * Apply all migrations greater than the current DB version.
 * - Reads current version (0 if table missing/empty)
 * - Applies migrations in order
 * - After each migration, persists `dbos_migrations.version` so partial
 *   progress is recorded
 * - Warns if current version > max known (likely newer software concurrently)
 */
export async function runSysMigrationsPg(
  client: ClientBase,
  allMigrations: ReadonlyArray<DBMigration>,
  schemaName: string = 'dbos',
  opts: PgMigratorOptions = {},
): Promise<{
  fromVersion: number;
  toVersion: number;
  appliedCount: number;
  skippedCount: number;
  notice?: string;
}> {
  const { ignoreErrorCodes = DEFAULT_IGNORABLE_CODES, onWarn = (m) => console.info(m), isCockroach = false } = opts;

  const current = await getCurrentSysDBVersion(client, schemaName);
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
  let loggedInfo = false;

  // Apply needed migrations in order
  for (let i = 0; i < allMigrations.length; i++) {
    const m = allMigrations[i];
    const v = i + 1;
    if (v <= current) {
      skipped++;
      continue;
    }

    if (!loggedInfo) {
      onWarn(`Running DBOS system database migrations...`);
      loggedInfo = true;
    }

    const stmts = m.pg ?? [];
    if (stmts.length === 0) {
      onWarn(`Migration "${m.name}" has no Postgres statements; skipping.`);
      skipped++;
      lastAppliedVersion = v;
      continue;
    }

    // Run migrations in autocommit
    if (m.online && !isCockroach) {
      // Drop any indexes left INVALID by a prior interrupted run before
      // attempting the next CONCURRENTLY statement.
      await cleanupInvalidIndexes(client, schemaName, (msg) => onWarn(msg));
      for (const s of stmts) {
        await client.query(s, []);
      }
    } else {
      await runStatementsIgnoring(client, stmts, ignoreErrorCodes, (msg, e) =>
        onWarn(`${msg}${e ? `\n  → ${String((e as { message?: string }).message ?? '')}` : ''}`),
      );
    }
    await bumpMigrationVersion(client, schemaName, v);
    applied++;
    lastAppliedVersion = v;
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
