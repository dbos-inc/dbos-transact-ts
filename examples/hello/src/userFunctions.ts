import { TransactionContext, OperonTransaction, GetApi, HandlerContext } from '@dbos-inc/operon'
import { Knex } from 'knex';

type KnexTransactionContext = TransactionContext<Knex>;

interface operon_hello {
  name: string;
  greet_count: number;
}

export class Hello {

  @OperonTransaction()
  static async helloTransaction(txnCtxt: KnexTransactionContext, name: string) {
    // Look up greet_count.
    let greet_count = await txnCtxt.client<operon_hello>("operon_hello")
      .select("greet_count")
      .where({ name: name })
      .first()
      .then(row => row?.greet_count);
    if (greet_count) {
      // If greet_count is set, increment it.
      greet_count++;
      await txnCtxt.client<operon_hello>("operon_hello")
        .where({ name: name })
        .increment('greet_count', 1);
    } else {
      // If greet_count is not set, set it to 1.
      greet_count = 1;
      await txnCtxt.client<operon_hello>("operon_hello")
        .insert({ name: name, greet_count: 1 })
    }
    return `Hello, ${name}! You have been greeted ${greet_count} times.\n`;
  }

  @GetApi('/greeting/:name')
  static async helloHandler(handlerCtxt: HandlerContext, name: string) {
    return handlerCtxt.invoke(Hello).helloTransaction(name);
  }
}
