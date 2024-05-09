import { Kafka as KafkaJS, Consumer, ConsumerConfig, KafkaConfig, KafkaMessage, KafkaJSProtocolError, ConsumerSubscribeTopics } from "kafkajs";
import { DBOSContext } from "..";
import { ClassRegistration, MethodRegistration, RegistrationDefaults, getOrCreateClassRegistration, registerAndWrapFunction } from "../decorators";
import { DBOSExecutor } from "../dbos-executor";
import { Transaction } from "../transaction";
import { Workflow } from "../workflow";
import { DBOSError } from "../error";
import { sleep } from "../utils";
import { randomUUID } from "crypto";

/**
 * ${1:Description placeholder}
 *
 * @typedef {KafkaArgs}
 */
type KafkaArgs = [string, number, KafkaMessage]

/////////////////////////////
/* Kafka Method Decorators */
/////////////////////////////

/**
 * ${1:Description placeholder}
 *
 * @export
 * @class KafkaRegistration
 * @typedef {KafkaRegistration}
 * @template This
 * @template {unknown[]} Args
 * @template Return
 * @extends {MethodRegistration<This, Args, Return>}
 */
export class KafkaRegistration<This, Args extends unknown[], Return> extends MethodRegistration<This, Args, Return> {
  /**
 * ${1:Description placeholder}
 *
 * @type {?(string | RegExp | Array<string | RegExp>)}
 */
kafkaTopic?: string | RegExp | Array<string | RegExp>;
  /**
 * ${1:Description placeholder}
 *
 * @type {?ConsumerConfig}
 */
consumerConfig?: ConsumerConfig;

  /**
 * Creates an instance of KafkaRegistration.
 *
 * @constructor
 * @param {(this: This, ...args: Args) => Promise<Return>} origFunc
 */
constructor(origFunc: (this: This, ...args: Args) => Promise<Return>) {
    super(origFunc);
  }
}

/**
 * ${1:Description placeholder}
 *
 * @export
 * @param {(string | RegExp | Array<string | RegExp>)} topic
 * @param {?ConsumerConfig} [consumerConfig]
 * @returns {<This, Ctx extends DBOSContext, Return>(target: object, propertyKey: string, inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, args_0: string, args_1: number, args_2: KafkaMessage) => Promise<Return>>) => any}
 */
export function KafkaConsume(topic: string | RegExp | Array<string | RegExp>, consumerConfig?: ConsumerConfig) {
  function kafkadec<This, Ctx extends DBOSContext, Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: KafkaArgs) => Promise<Return>>
  ) {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    const kafkaRegistration = registration as unknown as KafkaRegistration<This, KafkaArgs, Return>;
    kafkaRegistration.kafkaTopic = topic;
    kafkaRegistration.consumerConfig = consumerConfig;

    return descriptor;
  }
  return kafkadec;
}

/////////////////////////////
/* Kafka Class Decorators  */
/////////////////////////////

/**
 * ${1:Description placeholder}
 *
 * @export
 * @interface KafkaDefaults
 * @typedef {KafkaDefaults}
 * @extends {RegistrationDefaults}
 */
export interface KafkaDefaults extends RegistrationDefaults {
  /**
 * ${1:Description placeholder}
 *
 * @type {?KafkaConfig}
 */
kafkaConfig?: KafkaConfig;
}

/**
 * ${1:Description placeholder}
 *
 * @export
 * @class KafkaClassRegistration
 * @typedef {KafkaClassRegistration}
 * @template {{ new(...args: unknown[]): object }} CT
 * @extends {ClassRegistration<CT>}
 * @implements {KafkaDefaults\}
 */
export class KafkaClassRegistration<CT extends { new(...args: unknown[]): object }> extends ClassRegistration<CT> implements KafkaDefaults {
  /**
 * ${1:Description placeholder}
 *
 * @type {?KafkaConfig}
 */
kafkaConfig?: KafkaConfig;

  /**
 * Creates an instance of KafkaClassRegistration.
 *
 * @constructor
 * @param {CT} ctor
 */
constructor(ctor: CT) {
    super(ctor);
  }
}

/**
 * ${1:Description placeholder}
 *
 * @export
 * @param {KafkaConfig} kafkaConfig
 * @returns {<T extends new (...args: {}) => object>(ctor: T) => void\}
 */
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

/**
 * ${1:Description placeholder}
 *
 * @export
 * @class DBOSKafka
 * @typedef {DBOSKafka}
 */
export class DBOSKafka {
  /**
 * ${1:Description placeholder}
 *
 * @readonly
 * @type {Consumer[]}
 */
readonly consumers: Consumer[] = [];

  /**
 * Creates an instance of DBOSKafka.
 *
 * @constructor
 * @param {DBOSExecutor} dbosExec
 */
constructor(readonly dbosExec: DBOSExecutor) { }

  /**
 * ${1:Description placeholder}
 *
 * @async
 * @returns {*}
 */
async initKafka() {
    for (const registeredOperation of this.dbosExec.registeredOperations) {
      const ro = registeredOperation as KafkaRegistration<unknown, unknown[], unknown>;
      if (ro.kafkaTopic) {
        const defaults = ro.defaults as KafkaDefaults;
        if (!ro.txnConfig && !ro.workflowConfig) {
          throw new DBOSError(`Error registering method ${defaults.name}.${ro.name}: A Kafka decorator can only be assigned to a transaction or workflow!`)
        }
        if (!defaults.kafkaConfig) {
          throw new DBOSError(`Error registering method ${defaults.name}.${ro.name}: Kafka configuration not found. Does class ${defaults.name} have an @Kafka decorator?`)
        }
        const topics: Array<string | RegExp> = [];
        if (Array.isArray(ro.kafkaTopic) ) {
          topics.push(...ro.kafkaTopic)
        } else
        if (ro.kafkaTopic) {
          topics.push(ro.kafkaTopic)
        }
        const kafka = new KafkaJS(defaults.kafkaConfig);
        const uuid = randomUUID()
        const consumerConfig = ro.consumerConfig ?? { groupId: `dbos-kafka-group-${uuid}` };
        const consumer = kafka.consumer(consumerConfig);
        await consumer.connect();
        // A temporary workaround for https://github.com/tulios/kafkajs/pull/1558 until it gets fixed
        // If topic autocreation is on and you try to subscribe to a nonexistent topic, KafkaJS should retry until the topic is created.
        // However, it has a bug where it won't. Thus, we retry instead.
        const maxRetries = defaults.kafkaConfig.retry ? defaults.kafkaConfig.retry.retries ?? 5 : 5;
        let retryTime = defaults.kafkaConfig.retry ? defaults.kafkaConfig.retry.maxRetryTime ?? 300 : 300;
        const multiplier = defaults.kafkaConfig.retry ? defaults.kafkaConfig.retry.multiplier ?? 2 : 2;
        console.log(topics)
        for (let i = 0; i < maxRetries; i++) {
          try {
            const subscribeOpts: ConsumerSubscribeTopics = {
              topics: topics,
              fromBeginning: true
            }
            await consumer.subscribe(subscribeOpts);
            break;
          } catch (error) {
            const e = error as KafkaJSProtocolError;
            if (e.code === 3 && i + 1 < maxRetries) { // UNKNOWN_TOPIC_OR_PARTITION
              console.log(`received error. Retrying after ${retryTime}`)
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

  /**
 * ${1:Description placeholder}
 *
 * @async
 * @returns {*}
 */
async destroyKafka() {
    for (const consumer of this.consumers) {
      await consumer.disconnect();
    }
  }

  /**
 * ${1:Description placeholder}
 */
logRegisteredKafkaEndpoints() {
    const logger = this.dbosExec.logger;
    logger.info("Kafka endpoints supported:");
    this.dbosExec.registeredOperations.forEach((registeredOperation) => {
      const ro = registeredOperation as KafkaRegistration<unknown, unknown[], unknown>;
      if (ro.kafkaTopic) {
        const defaults = ro.defaults as KafkaDefaults;
        if (Array.isArray(ro.kafkaTopic)) {
          ro.kafkaTopic.forEach( kafkaTopic => {
            logger.info(`    ${kafkaTopic} -> ${defaults.name}.${ro.name}`);
          });
        } else {
          logger.info(`    ${ro.kafkaTopic} -> ${defaults.name}.${ro.name}`);
        }
      }
    });
  }
}
