process.env.PGPASSWORD = process.env.PGPASSWORD ?? 'dbos';
// By default, tests run with dual write off
process.env.DBOS__DUAL_WRITE_PAYLOADS = 'false';
process.env.FOO = 'foo';
process.env.BAR = 'bar';
process.env.BAZ = 'baz';
process.env.C = 'c';
