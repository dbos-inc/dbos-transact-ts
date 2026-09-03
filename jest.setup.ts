process.env.PGPASSWORD = process.env.PGPASSWORD ?? 'dbos';
// Every test runs with dual write off, so the payload tables are the only place a payload
// lives and a broken new-table path cannot hide behind the legacy columns. Tests that need
// the shipped default turn it back on around their own launch.
process.env.DBOS__DUAL_WRITE_PAYLOADS = process.env.DBOS__DUAL_WRITE_PAYLOADS ?? 'false';
process.env.FOO = 'foo';
process.env.BAR = 'bar';
process.env.BAZ = 'baz';
process.env.C = 'c';
