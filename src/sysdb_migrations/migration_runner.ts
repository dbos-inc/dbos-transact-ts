import type { SqlRunner, SqlStatement } from './migration_types';

// Execute a list of statements sequentially with optional transaction
export async function runStatements(
  runner: SqlRunner,
  stmts: SqlStatement[],
  { useTransaction = true }: { useTransaction?: boolean } = {},
) {
  let inTx = false;
  try {
    if (useTransaction && runner.begin && runner.commit && runner.rollback) {
      await runner.begin();
      inTx = true;
    }
    for (const s of stmts) await runner.exec(s.sql, s.bindings ?? null);
    if (inTx) await runner.commit!();
  } catch (e) {
    if (inTx)
      try {
        await runner.rollback!();
      } catch {}
    throw e;
  }
}
