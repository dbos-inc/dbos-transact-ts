export type RecordedStatement = { sql: string; bindings?: unknown[] };

export type SqlStatement = { sql: string; bindings?: unknown[] };

export type GeneratedMigration = {
  name: string;
  up: { pg?: ReadonlyArray<SqlStatement>; sqlite3?: ReadonlyArray<SqlStatement> };
  down: { pg?: ReadonlyArray<SqlStatement>; sqlite3?: ReadonlyArray<SqlStatement> };
};
