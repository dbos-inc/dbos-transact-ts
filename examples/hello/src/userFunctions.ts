import { TransactionContext, OperonTransaction, GetApi } from '@dbos-inc/operon'

export class Hello {
  @GetApi('/greeting/:name')
  @OperonTransaction()
  static async helloFunction(txnCtxt: TransactionContext, name: string) {
    const greeting = `Hello, ${name}!`
    const { rows } = await txnCtxt.pgClient.query<{ greeting_id: number }>("INSERT INTO OperonHello(greeting) VALUES ($1) RETURNING greeting_id", [greeting])
    return `Greeting ${rows[0].greeting_id}: Hello, ${name}!`;
  }
}
