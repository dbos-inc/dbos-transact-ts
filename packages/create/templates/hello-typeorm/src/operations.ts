import { TransactionContext, Transaction, GetApi, OrmEntities } from '@dbos-inc/dbos-sdk';
import { EntityManager } from "typeorm";
import { DBOSHello } from '../entities/DBOSHello';

@OrmEntities([DBOSHello])
export class Hello {

  @GetApi('/greeting/:name')
  @Transaction()
  static async helloTransaction(txnCtxt: TransactionContext<EntityManager>, name: string) {
    const greeting = `Hello, ${name}!`;
    let entity = new DBOSHello();
    entity.greeting = greeting;
    entity = await txnCtxt.client.save(entity);
    return `Greeting ${entity.greeting_id}: ${greeting}`;
  }
}
