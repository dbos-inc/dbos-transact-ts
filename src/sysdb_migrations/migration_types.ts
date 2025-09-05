export type GeneratedMigration = {
  name?: string;
  up: { pg?: ReadonlyArray<string>; sqlite3?: ReadonlyArray<string> };
  down: { pg?: ReadonlyArray<string>; sqlite3?: ReadonlyArray<string> };
};
