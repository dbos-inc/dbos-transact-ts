export type RecordedStatement = { sql: string; bindings?: unknown[] };

export type SqlStatement = { sql: string; bindings?: unknown[] };

export type SqlRunner = {
  // Execute a single statement (parameterized).
  exec(sql: string, bindings?: unknown[] | null): Promise<void>;

  // (Optional) transactional helpers:
  begin?(): Promise<void>;
  commit?(): Promise<void>;
  rollback?(): Promise<void>;
};

export type GeneratedMigration = {
  name: string;
  up: { pg?: ReadonlyArray<SqlStatement>; sqlite3?: ReadonlyArray<SqlStatement> };
  down: { pg?: ReadonlyArray<SqlStatement>; sqlite3?: ReadonlyArray<SqlStatement> };
};
