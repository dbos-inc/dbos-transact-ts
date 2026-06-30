import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

import type { GlobalLogger } from './telemetry/logs';
import { allMigrations } from './sysdb_migrations/internal/migrations';

type SQLiteValue = string | number | bigint | boolean | null | Buffer;
type SQLiteParams = SQLiteValue[];

export type SQLiteQueryResult<T = unknown> = {
  rows: T[];
  rowCount: number;
};

type SQLiteRunResult = {
  changes: number;
};

type SQLitePoolWaiter = {
  resolve: (release: () => void) => void;
  reject: (error: Error) => void;
};

function sqliteNowMsExpr(): string {
  return "(CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))";
}

export function isSQLiteSystemDatabaseUrl(databaseUrl: string): boolean {
  return databaseUrl.startsWith('sqlite:');
}

function sqliteFileFromUrl(databaseUrl: string): string {
  if (databaseUrl === 'sqlite:///:memory:' || databaseUrl === 'sqlite::memory:' || databaseUrl === 'sqlite://') {
    return ':memory:';
  }
  if (databaseUrl.startsWith('sqlite:////')) {
    return `/${databaseUrl.slice('sqlite:////'.length)}`;
  }
  if (databaseUrl.startsWith('sqlite:///')) {
    return databaseUrl.slice('sqlite:///'.length);
  }
  if (databaseUrl.startsWith('sqlite://')) {
    return databaseUrl.slice('sqlite://'.length);
  }
  if (databaseUrl.startsWith('sqlite:')) {
    return databaseUrl.slice('sqlite:'.length);
  }
  throw new Error(`Invalid SQLite database URL: ${databaseUrl}`);
}

function normalizeValue(value: unknown): SQLiteValue {
  if (value === undefined) return null;
  if (typeof value === 'boolean') return value ? 1 : 0;
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'bigint' ||
    Buffer.isBuffer(value)
  ) {
    return value;
  }
  return JSON.stringify(value);
}

export class SQLiteClient {
  #released = false;

  constructor(
    private readonly db: Database.Database,
    private readonly schemaName: string,
    private readonly releaseLock?: () => void,
  ) {}

  query<T = unknown>(sql: string, params: unknown[] = []): Promise<SQLiteQueryResult<T>> {
    const normalized = sql.trim();
    const upper = normalized.toUpperCase();

    if (upper === 'BEGIN' || upper.startsWith('BEGIN ISOLATION LEVEL')) {
      this.db.exec('BEGIN IMMEDIATE');
      return Promise.resolve({ rows: [], rowCount: 0 });
    }
    if (upper === 'COMMIT' || upper === 'ROLLBACK') {
      this.db.exec(upper);
      return Promise.resolve({ rows: [], rowCount: 0 });
    }
    if (upper.startsWith('LISTEN ') || upper.startsWith('NOTIFY ')) {
      return Promise.resolve({ rows: [], rowCount: 0 });
    }
    if (upper === 'SELECT VERSION() AS VERSION') {
      return Promise.resolve({ rows: [{ version: 'SQLite' } as T], rowCount: 1 });
    }

    try {
      const { sql: sqliteSql, params: sqliteParams } = translateQuery(normalized, params, this.schemaName);
      const stmt = this.db.prepare(sqliteSql);

      if (stmt.reader) {
        const rows = stmt.all(...sqliteParams) as T[];
        return Promise.resolve({ rows, rowCount: rows.length });
      }

      const result = stmt.run(...sqliteParams) as SQLiteRunResult;
      return Promise.resolve({ rows: [], rowCount: result.changes });
    } catch (e) {
      return Promise.reject(mapSQLiteError(e));
    }
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.releaseLock?.();
  }
  on(): this {
    return this;
  }
  removeListener(): this {
    return this;
  }
}

export class SQLitePool {
  readonly options: { max: number };
  private readonly db: Database.Database;
  private locked = false;
  private readonly waiters: SQLitePoolWaiter[] = [];
  private ended = false;

  constructor(
    databaseUrl: string,
    readonly schemaName: string = 'dbos',
    max: number = 1,
  ) {
    this.options = { max };
    const file = sqliteFileFromUrl(databaseUrl);
    this.db = new Database(file === ':memory:' ? file : path.resolve(file));
    this.db.pragma('busy_timeout = 30000');
    this.db.pragma('foreign_keys = ON');
    this.db.pragma('journal_mode = WAL');
  }

  async query<T = unknown>(sql: string, params: unknown[] = []): Promise<SQLiteQueryResult<T>> {
    const release = await this.acquire();
    const client = new SQLiteClient(this.db, this.schemaName, release);
    try {
      return await client.query<T>(sql, params);
    } finally {
      client.release();
    }
  }

  async connect(): Promise<SQLiteClient> {
    const release = await this.acquire();
    return new SQLiteClient(this.db, this.schemaName, release);
  }

  end(): Promise<void> {
    if (this.ended) {
      return Promise.resolve();
    }

    this.ended = true;
    const endedError = sqlitePoolEndedError();
    for (const waiter of this.waiters.splice(0)) {
      waiter.reject(endedError);
    }
    this.db.close();
    return Promise.resolve();
  }

  on(): this {
    return this;
  }

  private acquire(): Promise<() => void> {
    if (this.ended) {
      return Promise.reject(new Error('SQLite system database pool has ended'));
    }

    if (!this.locked) {
      this.locked = true;
      return Promise.resolve(this.makeRelease());
    }

    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  private makeRelease(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = this.waiters.shift();
      if (next) {
        next.resolve(this.makeRelease());
      } else {
        this.locked = false;
      }
    };
  }
}

function sqlitePoolEndedError(): Error {
  return new Error('SQLite system database pool has ended');
}

export async function ensureSQLiteSystemDatabase(
  sysDbUrl: string,
  logger: GlobalLogger,
  schemaName: string = 'dbos',
): Promise<void> {
  const pool = new SQLitePool(sysDbUrl, schemaName);
  try {
    await createCurrentSQLiteSchema(pool, logger);
  } finally {
    await pool.end();
  }
}

export function resetSQLiteSystemDatabase(sysDbUrl: string, logger?: GlobalLogger): void {
  const file = sqliteFileFromUrl(sysDbUrl);
  if (file === ':memory:') {
    logger?.info('SQLite in-memory system database does not need file reset.');
    return;
  }

  const dbPath = path.resolve(file);
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
    logger?.info(`Deleted SQLite database file: ${dbPath}`);
  } else {
    logger?.info(`SQLite database file does not exist: ${dbPath}`);
  }
}

async function createCurrentSQLiteSchema(pool: SQLitePool, logger: GlobalLogger): Promise<void> {
  logger.info('Ensuring DBOS SQLite system database schema...');

  const client = await pool.connect();
  let inTransaction = false;
  try {
    await client.query('BEGIN');
    inTransaction = true;
    for (const statement of currentSQLiteSchemaStatements()) {
      await client.query(statement);
    }
    await client.query(`DELETE FROM dbos_migrations`);
    await client.query(`INSERT INTO dbos_migrations (version) VALUES ($1)`, [allMigrations().length]);
    await client.query('COMMIT');
    inTransaction = false;
  } catch (e) {
    if (inTransaction) {
      await client.query('ROLLBACK').catch(() => undefined);
    }
    throw e;
  } finally {
    client.release();
  }
}

function currentSQLiteSchemaStatements(): ReadonlyArray<string> {
  const now = sqliteNowMsExpr();
  return [
    `CREATE TABLE IF NOT EXISTS dbos_migrations (version INTEGER NOT NULL PRIMARY KEY)`,
    `CREATE TABLE IF NOT EXISTS workflow_status (
      workflow_uuid TEXT PRIMARY KEY,
      status TEXT,
      name TEXT,
      authenticated_user TEXT,
      assumed_role TEXT,
      authenticated_roles TEXT,
      request TEXT,
      output TEXT,
      error TEXT,
      executor_id TEXT,
      created_at INTEGER NOT NULL DEFAULT ${now},
      updated_at INTEGER NOT NULL DEFAULT ${now},
      application_version TEXT,
      application_id TEXT,
      class_name TEXT DEFAULT NULL,
      config_name TEXT DEFAULT NULL,
      recovery_attempts INTEGER DEFAULT 0,
      queue_name TEXT,
      workflow_timeout_ms INTEGER,
      workflow_deadline_epoch_ms INTEGER,
      inputs TEXT,
      started_at_epoch_ms INTEGER,
      deduplication_id TEXT,
      priority INTEGER NOT NULL DEFAULT 0,
      queue_partition_key TEXT,
      forked_from TEXT,
      owner_xid TEXT DEFAULT NULL,
      parent_workflow_id TEXT DEFAULT NULL,
      serialization TEXT DEFAULT NULL,
      delay_until_epoch_ms INTEGER DEFAULT NULL,
      was_forked_from BOOLEAN NOT NULL DEFAULT FALSE,
      rate_limited BOOLEAN NOT NULL DEFAULT FALSE,
      completed_at INTEGER,
      attributes TEXT,
      schedule_name TEXT
    )`,
    `CREATE TABLE IF NOT EXISTS operation_outputs (
      workflow_uuid TEXT NOT NULL,
      function_id INTEGER NOT NULL,
      function_name TEXT NOT NULL DEFAULT '',
      output TEXT,
      error TEXT,
      child_workflow_id TEXT,
      started_at_epoch_ms INTEGER,
      completed_at_epoch_ms INTEGER,
      serialization TEXT DEFAULT NULL,
      PRIMARY KEY (workflow_uuid, function_id),
      FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
    )`,
    `CREATE TABLE IF NOT EXISTS notifications (
      message_uuid TEXT NOT NULL DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY,
      destination_uuid TEXT NOT NULL,
      topic TEXT,
      message TEXT NOT NULL,
      created_at_epoch_ms INTEGER NOT NULL DEFAULT ${now},
      consumed BOOLEAN NOT NULL DEFAULT FALSE,
      serialization TEXT DEFAULT NULL,
      FOREIGN KEY (destination_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
    )`,
    `CREATE TABLE IF NOT EXISTS workflow_events (
      workflow_uuid TEXT NOT NULL,
      key TEXT NOT NULL,
      value TEXT NOT NULL,
      serialization TEXT DEFAULT NULL,
      PRIMARY KEY (workflow_uuid, key),
      FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
    )`,
    `CREATE TABLE IF NOT EXISTS event_dispatch_kv (
      service_name TEXT NOT NULL,
      workflow_fn_name TEXT NOT NULL,
      key TEXT NOT NULL,
      value TEXT,
      update_seq NUMERIC,
      update_time NUMERIC,
      PRIMARY KEY (service_name, workflow_fn_name, key)
    )`,
    `CREATE TABLE IF NOT EXISTS streams (
      workflow_uuid TEXT NOT NULL,
      key TEXT NOT NULL,
      value TEXT NOT NULL,
      "offset" INTEGER NOT NULL,
      function_id INTEGER NOT NULL DEFAULT 0,
      serialization TEXT DEFAULT NULL,
      PRIMARY KEY (workflow_uuid, key, "offset"),
      FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
    )`,
    `CREATE TABLE IF NOT EXISTS workflow_events_history (
      workflow_uuid TEXT NOT NULL,
      function_id INTEGER NOT NULL,
      key TEXT NOT NULL,
      value TEXT NOT NULL,
      serialization TEXT DEFAULT NULL,
      PRIMARY KEY (workflow_uuid, function_id, key),
      FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
    )`,
    `CREATE TABLE IF NOT EXISTS scheduler_state (
      workflow_fn_name TEXT NOT NULL PRIMARY KEY,
      last_run_time INTEGER NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS workflow_schedules (
      schedule_id TEXT PRIMARY KEY,
      schedule_name TEXT NOT NULL UNIQUE,
      workflow_name TEXT NOT NULL,
      workflow_class_name TEXT,
      schedule TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'ACTIVE',
      context TEXT NOT NULL,
      last_fired_at TEXT DEFAULT NULL,
      automatic_backfill BOOLEAN NOT NULL DEFAULT FALSE,
      cron_timezone TEXT DEFAULT NULL,
      queue_name TEXT DEFAULT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS application_versions (
      version_id TEXT NOT NULL PRIMARY KEY,
      version_name TEXT NOT NULL UNIQUE,
      version_timestamp INTEGER NOT NULL DEFAULT ${now},
      created_at INTEGER NOT NULL DEFAULT ${now}
    )`,
    `CREATE TABLE IF NOT EXISTS queues (
      queue_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
      name TEXT NOT NULL UNIQUE,
      concurrency INTEGER,
      worker_concurrency INTEGER,
      rate_limit_max INTEGER,
      rate_limit_period_sec REAL,
      priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
      partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
      polling_interval_sec REAL NOT NULL DEFAULT 1.0,
      created_at INTEGER NOT NULL DEFAULT ${now},
      updated_at INTEGER NOT NULL DEFAULT ${now}
    )`,
    `CREATE INDEX IF NOT EXISTS workflow_status_created_at_index ON workflow_status (created_at)`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_pending ON workflow_status (created_at) WHERE status = 'PENDING'`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_failed ON workflow_status (status, created_at) WHERE status IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_in_flight ON workflow_status (queue_name, status, priority, created_at) WHERE status IN ('ENQUEUED', 'PENDING')`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_rate_limited ON workflow_status (queue_name, started_at_epoch_ms) WHERE rate_limited = TRUE`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_delayed ON workflow_status (delay_until_epoch_ms) WHERE status = 'DELAYED'`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_started_at ON workflow_status (started_at_epoch_ms) WHERE started_at_epoch_ms IS NOT NULL`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_completed_at ON workflow_status (completed_at) WHERE completed_at IS NOT NULL`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_forked_from ON workflow_status (forked_from) WHERE forked_from IS NOT NULL`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_parent_workflow_id ON workflow_status (parent_workflow_id) WHERE parent_workflow_id IS NOT NULL`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_status_schedule_name ON workflow_status (schedule_name) WHERE schedule_name IS NOT NULL`,
    `CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_status_dedup_id ON workflow_status (queue_name, deduplication_id) WHERE deduplication_id IS NOT NULL`,
    `CREATE INDEX IF NOT EXISTS idx_workflow_topic ON notifications (destination_uuid, topic)`,
    `CREATE INDEX IF NOT EXISTS idx_notifications ON notifications (destination_uuid, topic)`,
    `CREATE INDEX IF NOT EXISTS idx_operation_outputs_completed_at_function_name ON operation_outputs (completed_at_epoch_ms, function_name)`,
  ];
}

function translateQuery(sql: string, params: unknown[], schemaName: string): { sql: string; params: SQLiteParams } {
  const extras = new Map<string, SQLiteValue>();
  let extraCounter = 0;
  let translated = sql
    .replace(new RegExp(`"${schemaName}"\\."([^"]+)"`, 'g'), '"$1"')
    .replace(new RegExp(`"${schemaName}"\\.`, 'g'), '')
    .replace(new RegExp(`\\b${schemaName}\\.`, 'g'), '')
    .replace(/BEGIN ISOLATION LEVEL (?:READ COMMITTED|REPEATABLE READ)/gi, 'BEGIN IMMEDIATE')
    .replace(/FOR UPDATE(?: SKIP LOCKED| NOWAIT)?/gi, '')
    .replace(/::(?:text\[\]|jsonb|bigint|int4|integer|int|text|json)/gi, '')
    .replace(/\bINT4\b/gi, 'INTEGER')
    .replace(/\bBIGINT\b/gi, 'INTEGER')
    .replace(/\bDOUBLE PRECISION\b/gi, 'REAL')
    .replace(/\bJSONB\b/gi, 'TEXT')
    .replace(/\bgen_random_uuid\(\)::TEXT\b/gi, () => `'${randomUUID()}'`)
    .replace(/\buuid_generate_v4\(\)\b/gi, () => `'${randomUUID()}'`)
    .replace(/\(EXTRACT\(EPOCH FROM now\(\)\)\s*\*\s*1000(?:\.0)?\)::bigint/gi, sqliteNowMsExpr())
    .replace(/\(EXTRACT\(epoch FROM now\(\)\s*\)\s*\*\s*1000(?:\.0)?\)::bigint/gi, sqliteNowMsExpr())
    .replace(/EXTRACT\(epoch FROM now\(\)\)\s*\*\s*1000/gi, sqliteNowMsExpr())
    .replace(/RETURNING\s+notifications\./gi, 'RETURNING ')
    .replace(/RETURNING\s+workflow_events\./gi, 'RETURNING ')
    .replace(/\bnotifications\./g, '')
    .replace(/\bworkflow_events\./g, '')
    .replace(
      /\bGREATEST\(EXCLUDED\.update_time,\s*event_dispatch_kv\.update_time\)/gi,
      `CASE
        WHEN event_dispatch_kv.update_time IS NULL THEN EXCLUDED.update_time
        WHEN EXCLUDED.update_time IS NULL THEN event_dispatch_kv.update_time
        WHEN EXCLUDED.update_time > event_dispatch_kv.update_time THEN EXCLUDED.update_time
        ELSE event_dispatch_kv.update_time
      END`,
    )
    .replace(
      /\bGREATEST\(EXCLUDED\.update_seq,\s*event_dispatch_kv\.update_seq\)/gi,
      `CASE
        WHEN event_dispatch_kv.update_seq IS NULL THEN EXCLUDED.update_seq
        WHEN EXCLUDED.update_seq IS NULL THEN event_dispatch_kv.update_seq
        WHEN EXCLUDED.update_seq > event_dispatch_kv.update_seq THEN EXCLUDED.update_seq
        ELSE event_dispatch_kv.update_seq
      END`,
    )
    .replace(/\bTRUE\b/g, '1')
    .replace(/\bFALSE\b/g, '0');

  if (translated.includes(' @> ')) {
    throw new Error(
      'Filtering workflows by attributes is not supported on SQLite. Use a Postgres system database to filter by attributes.',
    );
  }

  translated = translated.replace(
    /([A-Za-z0-9_".]+)\s*=\s*ANY\(\$(\d+)\)/g,
    (_match, column: string, index: string) => {
      const value = getPositionalParam(params, index);
      const values = Array.isArray(value) ? value : [value];
      if (values.length === 0) return '1 = 0';
      const tokens = values.map((v) => {
        const token = `__sqlite_extra_${extraCounter++}__`;
        extras.set(token, normalizeValue(v));
        return token;
      });
      return `${column} IN (${tokens.join(', ')})`;
    },
  );

  const sqliteParams: SQLiteParams = [];
  translated = translated.replace(/\$(\d+)|__sqlite_extra_\d+__/g, (token, index: string | undefined) => {
    if (token.startsWith('__sqlite_extra_')) {
      sqliteParams.push(getExtraParam(extras, token));
      return '?';
    }
    if (index === undefined) {
      throw new Error(`Missing SQLite query parameter index for token ${token}`);
    }
    sqliteParams.push(normalizeValue(getPositionalParam(params, index)));
    return '?';
  });

  return { sql: translated, params: sqliteParams };
}

function getPositionalParam(params: unknown[], index: string): unknown {
  const position = Number(index) - 1;
  if (!Number.isInteger(position) || position < 0 || position >= params.length) {
    throw new Error(`Missing SQLite query parameter $${index}`);
  }
  return params[position];
}

function getExtraParam(extras: ReadonlyMap<string, SQLiteValue>, token: string): SQLiteValue {
  if (!extras.has(token)) {
    throw new Error(`Missing generated SQLite query parameter ${token}`);
  }
  return extras.get(token) ?? null;
}

function mapSQLiteError(e: unknown): Error {
  if (!(e instanceof Error)) return new Error(String(e));
  const err = e as Error & { code?: string };
  if (err.code === 'SQLITE_CONSTRAINT_UNIQUE' || err.code === 'SQLITE_CONSTRAINT_PRIMARYKEY') {
    err.code = '23505';
  } else if (err.code === 'SQLITE_CONSTRAINT_FOREIGNKEY') {
    err.code = '23503';
  } else if (err.code === 'SQLITE_BUSY' || err.message.includes('database is locked')) {
    err.code = '55P03';
  }
  return err;
}
