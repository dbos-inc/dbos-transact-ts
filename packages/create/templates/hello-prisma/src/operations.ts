import { TransactionContext, Transaction, GetApi } from '@dbos-inc/dbos-sdk';
import { PrismaClient } from "@prisma/client";

export class Hello {

  @GetApi('/greeting/:name')
  @Transaction()
  static async helloTransaction(txnCtxt: TransactionContext<PrismaClient>, name: string)  {
    const greeting = `Hello, ${name}!`;
    const p: PrismaClient = txnCtxt.client as PrismaClient;
    const res = await p.dBOSHello.create({
      data: {
        greeting: greeting,
      },
    });
    return `Greeting ${res.greeting_id}: ${greeting}`;
  }
}
