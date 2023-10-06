import { TransactionContext, OperonTransaction, GetApi } from '@dbos-inc/operon'
import { PoolClient } from 'pg';

type PGTransactionContext = TransactionContext<PoolClient>;

export class Hello {
  @GetApi('/greeting/:name')
  @OperonTransaction()
  static async helloFunction(txnCtxt: PGTransactionContext, name: string) {
    const greeting = `Hello, ${name}!`
    txnCtxt.info(greeting);
    const { rows } = await txnCtxt.client.query<{ greeting_id: number }>("INSERT INTO OperonHello(greeting) VALUES ($1) RETURNING greeting_id", [greeting])
    return `Greeting ${rows[0].greeting_id}: Hello, ${name}!`;
  }
}
