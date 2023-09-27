import { AsyncLocalStorage } from 'node:async_hooks';
import { TransactionContext, WorkflowContext, OperonTransaction, OperonWorkflow, GetApi, OperonContext } from 'operon'

export class Hello {

  @OperonTransaction()
  static async helloFunction(ctx: TransactionContext, name: string): Promise<string> {
    // for OperonTransaction/Communicator methods, wrap the call in an AsyncLocalStorage.run call 
    // so the function can retrieve the Transaction/CommunicatorContext via base class protected
    // properties that access the ALS instances.
    return await als.run({ context: ctx, kind: 'transaction' }, (): Promise<string> => {
      // create a new instance of the $Hello object and call the function
      // Note, every wf/tx/comm call will be on a different instance of the object
      const obj = new $Hello();
      return obj.helloFunction(name);
    })
  }

  @OperonWorkflow()
  @GetApi('/greeting/:name')
  static async helloWorkflow(ctx: WorkflowContext, name: string) {

    // create a custom this object that swaps existing tx/comm methods for ones that use ctx.transaction/external 
    const $this = {
      helloFunction: (name: string): Promise<string> => {
        return ctx.transaction(Hello.helloFunction, name);
      }
    };

    // wrap the call in the workflow context AsyncLocalStorage
    return await als.run({ context: ctx, kind: 'workflow' }, (): Promise<string> => {
      const obj = new $Hello();

      // use .call so we can swap in the custom this instance that handles all the tx/comm methods correctly
      return obj.helloWorkflow.call($this, name);
    })
  }
}

interface ContextAsyncLocal {
  readonly context: OperonContext;
  readonly kind: 'workflow' | 'transaction'
}

const als = new AsyncLocalStorage<ContextAsyncLocal>();

export class $OperonBase {
  #getContext() {
    const ctx = als.getStore();
    if (!ctx) throw new Error("missing AsyncLocalStorage context");
    return ctx;
  }
  protected get txCtx() {
    const { context, kind } = this.#getContext();
    if (kind !== 'transaction') throw new Error(`unexpected context kind: ${kind}`);
    return context as TransactionContext;
  }

  protected get wfCtx() {
    const { context, kind } = this.#getContext();
    if (kind !== 'workflow') throw new Error(`unexpected context kind: ${kind}`);
    return context as WorkflowContext;
  }
}

// This is the code we WANT developers to write
export class $Hello extends $OperonBase {
  // TODO: update @OperonTransaction for instance methods w/o TxCtx param
  async helloFunction(name: string) {
    const greeting = `Hello, ${name}!`

    // NOTE: transaction context available via base class protected property
    const { rows } = await this.txCtx.pgClient.query<{ greeting_id: number }>("INSERT INTO OperonHello(greeting) VALUES ($1) RETURNING greeting_id", [greeting])
    return `Greeting ${rows[0].greeting_id}: ${greeting}`;
  }

  // TODO: update @OperonWorkflow for instance methods w/o WfCtx param
  async helloWorkflow(name: string) {
    // NOTE: custom this object that wraps @OperonTransaction/Communicator in calls to ctx.transaction/external 
    return this.helloFunction(name);
  }
}


