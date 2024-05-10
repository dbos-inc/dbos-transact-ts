import { Kafka as KafkaJS, Consumer, ConsumerConfig, KafkaConfig, KafkaMessage, KafkaJSProtocolError } from "kafkajs";
import { DBOSContext } from "..";
import { ClassRegistration, MethodRegistration, RegistrationDefaults, getOrCreateClassRegistration, registerAndWrapFunction } from "../decorators";
import { DBOSExecutor } from "../dbos-executor";
import { Transaction } from "../transaction";
import { Workflow } from "../workflow";
import { DBOSError } from "../error";
import { sleep } from "../utils";

type KafkaArgs = [string, number, KafkaMessage]

/////////////////////////////
/* Kafka Method Decorators */
/////////////////////////////

export class KafkaRegistration<This, Args extends unknown[], Return> extends MethodRegistration<This, Args, Return> {
  kafkaTopics?: string | RegExp | Array<string | RegExp>;
  consumerConfig?: ConsumerConfig;

  constructor(origFunc: (this: This, ...args: Args) => Promise<Return>) {
    super(origFunc);
  }
}

export function KafkaConsume(topics: string | RegExp | Array<string | RegExp>, consumerConfig?: ConsumerConfig) {
  function kafkadec<This, Ctx extends DBOSContext, Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: KafkaArgs) => Promise<Return>>
  ) {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    const kafkaRegistration = registration as unknown as KafkaRegistration<This, KafkaArgs, Return>;
    kafkaRegistration.kafkaTopics = topics;
    kafkaRegistration.consumerConfig = consumerConfig;

    return descriptor;
  }
  return kafkadec;
}

/////////////////////////////
/* Kafka Class Decorators  */
/////////////////////////////

export interface KafkaDefaults extends RegistrationDefaults {
  kafkaConfig?: KafkaConfig;
}

export class KafkaClassRegistration<CT extends { new(...args: unknown[]): object }> extends ClassRegistration<CT> implements KafkaDefaults {
  kafkaConfig?: KafkaConfig;

  constructor(ctor: CT) {
    super(ctor);
  }
}

export function Kafka(kafkaConfig: KafkaConfig) {
  function clsdec<T extends { new(...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as KafkaClassRegistration<T>;
    clsreg.kafkaConfig = kafkaConfig;
  }
  return clsdec;
}

////////////////////////
/* Kafka Management  */
///////////////////////

export class DBOSKafka {
  readonly consumers: Consumer[] = [];

  constructor(readonly dbosExec: DBOSExecutor) { }

  async initKafka() {
    for (const registeredOperation of this.dbosExec.registeredOperations) {
      const ro = registeredOperation as KafkaRegistration<unknown, unknown[], unknown>;
      if (ro.kafkaTopics) {
        const defaults = ro.defaults as KafkaDefaults;
        if (!ro.txnConfig && !ro.workflowConfig) {
          throw new DBOSError(`Error registering method ${defaults.name}.${ro.name}: A Kafka decorator can only be assigned to a transaction or workflow!`)
        }
        if (!defaults.kafkaConfig) {
          throw new DBOSError(`Error registering method ${defaults.name}.${ro.name}: Kafka configuration not found. Does class ${defaults.name} have an @Kafka decorator?`)
        }
        const topics: Array<string | RegExp> = [];
        if (Array.isArray(ro.kafkaTopics) ) {
          topics.push(...ro.kafkaTopics)
        } else
        if (ro.kafkaTopics) {
          topics.push(ro.kafkaTopics)
        }
        const kafka = new KafkaJS(defaults.kafkaConfig);
        const consumerConfig = ro.consumerConfig ?? { groupId: `${this.safeGroupName(topics)}` };
        const consumer = kafka.consumer(consumerConfig);
        await consumer.connect();
        // A temporary workaround for https://github.com/tulios/kafkajs/pull/1558 until it gets fixed
        // If topic autocreation is on and you try to subscribe to a nonexistent topic, KafkaJS should retry until the topic is created.
        // However, it has a bug where it won't. Thus, we retry instead.
        const maxRetries = defaults.kafkaConfig.retry ? defaults.kafkaConfig.retry.retries ?? 5 : 5;
        let retryTime = defaults.kafkaConfig.retry ? defaults.kafkaConfig.retry.maxRetryTime ?? 300 : 300;
        const multiplier = defaults.kafkaConfig.retry ? defaults.kafkaConfig.retry.multiplier ?? 2 : 2;
        for (let i = 0; i < maxRetries; i++) {
          try {
            await consumer.subscribe({ topics: topics, fromBeginning: true });
            break;
          } catch (error) {
            const e = error as KafkaJSProtocolError;
            if (e.code === 3 && i + 1 < maxRetries) { // UNKNOWN_TOPIC_OR_PARTITION
              await sleep(retryTime);
              retryTime *= multiplier;
              continue;
            } else {
              throw e
            }
          }
        }
        await consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            // This combination uniquely identifies a message for a given Kafka cluster
            const workflowUUID = `kafka-unique-id-${topic}-${partition}-${message.offset}`
            const wfParams = { workflowUUID: workflowUUID };
            // All operations annotated with Kafka decorators must take in these three arguments
            const args: KafkaArgs = [topic, partition, message];
            // We can only guarantee exactly-once-per-message execution of transactions and workflows.
            if (ro.txnConfig) {
              // Execute the transaction
              await this.dbosExec.transaction(ro.registeredFunction as Transaction<unknown[], unknown>, wfParams, ...args);
            } else if (ro.workflowConfig) {
              // Safely start the workflow
              await this.dbosExec.workflow(ro.registeredFunction as Workflow<unknown[], unknown>, wfParams, ...args);
            }
          },
        })
        this.consumers.push(consumer);
      }
    }
  }

  async destroyKafka() {
    for (const consumer of this.consumers) {
      await consumer.disconnect();
    }
  }

  safeGroupName(topics: Array<string | RegExp>) {
    const safeGroupIdPart =  topics
      .map(r => r.toString())
      .map( r => r.replaceAll(/[^a-zA-Z0-9\\-]/g, ''))
      .join('-');
    return `dbos-kafka-group-${safeGroupIdPart}`.slice(0, 255);
  }

  logRegisteredKafkaEndpoints() {
    const logger = this.dbosExec.logger;
    logger.info("Kafka endpoints supported:");
    this.dbosExec.registeredOperations.forEach((registeredOperation) => {
      const ro = registeredOperation as KafkaRegistration<unknown, unknown[], unknown>;
      if (ro.kafkaTopics) {
        const defaults = ro.defaults as KafkaDefaults;
        if (Array.isArray(ro.kafkaTopics)) {
          ro.kafkaTopics.forEach( kafkaTopic => {
            logger.info(`    ${kafkaTopic} -> ${defaults.name}.${ro.name}`);
          });
        } else {
          logger.info(`    ${ro.kafkaTopics} -> ${defaults.name}.${ro.name}`);
        }
      }
    });
  }
}
