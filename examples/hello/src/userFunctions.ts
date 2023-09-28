import {
  TransactionContext,
  WorkflowContext,
  OperonTransaction,
  OperonWorkflow,
  GetApi,
} from "operon";

interface Animal {
  type: string;
  name: string;
  age: number;
}

export class Hello {
  @OperonTransaction()
  static async helloFunction(txnCtxt: TransactionContext, name: string) {
    const greeting = `Hello, ${name}!`;
    const { rows } = await txnCtxt.pgClient.query<{ greeting_id: number }>(
      "INSERT INTO OperonHello(greeting) VALUES ($1) RETURNING greeting_id",
      [greeting]
    );
    txnCtxt.warn(`Inserted greeting ${rows[0].greeting_id}: ${greeting}`);
    return `Greeting ${rows[0].greeting_id}: ${greeting}`;
  }

  @OperonWorkflow()
  @GetApi("/greeting/:name")
  static async helloWorkflow(workflowCtxt: WorkflowContext, name: string) {
    const fooObj = workflowCtxt.getConfig("foo");
    workflowCtxt.log(JSON.stringify(fooObj));
    const barVar = workflowCtxt.getConfig("foo.bar");
    workflowCtxt.log(`bar: ${barVar}`);
    const bazVar = workflowCtxt.getConfig("baz");
    workflowCtxt.log(`baz: ${bazVar}`);
    const animals = workflowCtxt.getConfig("animals") as Animal[];
    if (animals) {
      for (const animal of animals) {
        workflowCtxt.log(JSON.stringify(animal));
      }
    }
    return await workflowCtxt.transaction(Hello.helloFunction, name);
  }
}
