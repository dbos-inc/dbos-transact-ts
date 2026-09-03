process.env.PGPASSWORD = process.env.PGPASSWORD ?? 'dbos';
// By default, tests run with dual write off
process.env.DBOS__DUAL_WRITE_PAYLOADS = 'false';
