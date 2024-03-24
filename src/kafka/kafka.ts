import { Consumer } from "kafkajs";
import { DBOSContext } from "..";
import { MethodRegistration, registerAndWrapFunction } from "../decorators";
import { DBOSExecutor } from "../dbos-executor";
import { Transaction } from "../transaction";
import { Workflow } from "../workflow";

export class KafkaRegistration<This, Args extends unknown[], Return> extends MethodRegistration<This, Args, Return> {
  consumer: Consumer | undefined = undefined;

  constructor(origFunc: (this: This, ...args: Args) => Promise<Return>) {
    super(origFunc);
  }
}

export function KafkaConsume(consumer: Consumer) {
  function kafkadec<This, Ctx extends DBOSContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>
  ) {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    const kafkaRegistration = registration as unknown as KafkaRegistration<This, Args, Return>;
    kafkaRegistration.consumer = consumer;

    return descriptor;
  }
  return kafkadec;
}

// Register the Kafka decorators. Must run after the @DBOSInitializer functions have been executed.
export async function initKafka(dbosExec: DBOSExecutor) {
  for (const registeredOperation of dbosExec.registeredOperations) {
    const ro = registeredOperation as KafkaRegistration<unknown, unknown[], unknown>;
    if (ro.consumer) {
      await ro.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          // This combination uniquely identifies a message for a given Kafka cluster
          const workflowUUID = `kafka-unique-id-${topic}-${partition}-${message.offset}`
          const wfParams = { workflowUUID: workflowUUID };
          // All operations annotated with Kafka decorators must take in these three arguments
          const args = [topic, partition, message]
          // We can only guarantee exactly-once-per-message execution of transactions and workflows.
          if (ro.txnConfig) {
            // Execute the transaction
            await dbosExec.transaction(ro.registeredFunction as Transaction<unknown[], unknown>, wfParams, ...args);
          } else if (ro.workflowConfig) {
            // Safely start the workflow
            await dbosExec.workflow(ro.registeredFunction as Workflow<unknown[], unknown>, wfParams, ...args);
          }
        },
      })
    }
  }
}
