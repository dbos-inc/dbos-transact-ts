import { Kafka as KafkaJS, Consumer, ConsumerConfig, KafkaConfig, KafkaMessage, KafkaJSProtocolError } from "kafkajs";
import { DBOSContext, DBOSEventReceiver } from "..";
import { associateClassWithEventReceiver, associateMethodWithEventReceiver } from "..";
import { TransactionFunction } from "..";
import { WorkflowFunction } from "..";
import { Error as DBOSError } from "..";
import { sleepms } from "../utils";
import { DBOSExecutorContext } from "..";

type KafkaArgs = [string, number, KafkaMessage]

////////////////////////
/* Kafka Management  */
///////////////////////

export class DBOSKafka implements DBOSEventReceiver {
  readonly consumers: Consumer[] = [];

  executor?: DBOSExecutorContext = undefined;

  constructor() { }

  async initialize(dbosExecI: DBOSExecutorContext) {
    this.executor = dbosExecI;
    const regops = this.executor.getRegistrationsFor(this);
    for (const registeredOperation of regops) {
      const ro = registeredOperation.methodConfig as KafkaRegistrationInfo;
      if (ro.kafkaTopics) {
        const defaults = registeredOperation.classConfig as KafkaDefaults;
        const method = registeredOperation.methodReg;
        const cname = method.className;
        const mname = method.name;
        if (!method.txnConfig && !method.workflowConfig) {
          throw new DBOSError.DBOSError(`Error registering method ${cname}.${mname}: A Kafka decorator can only be assigned to a transaction or workflow!`)
        }
        if (!defaults.kafkaConfig) {
          throw new DBOSError.DBOSError(`Error registering method ${cname}.${mname}: Kafka configuration not found. Does class ${cname} have an @Kafka decorator?`)
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
              await sleepms(retryTime);
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
            const wfParams = { workflowUUID: workflowUUID, configuredInstance: null, queueName: ro.queueName};
            // All operations annotated with Kafka decorators must take in these three arguments
            const args: KafkaArgs = [topic, partition, message];
            // We can only guarantee exactly-once-per-message execution of transactions and workflows.
            if (method.txnConfig) {
              // Execute the transaction
              await this.executor!.transaction(method.registeredFunction as TransactionFunction<unknown[], unknown>, wfParams, ...args);
            } else if (method.workflowConfig) {
              // Safely start the workflow
              await this.executor!.workflow(method.registeredFunction as unknown as WorkflowFunction<unknown[], unknown>, wfParams, ...args);
            }
          },
        })
        this.consumers.push(consumer);
      }
    }
  }

  async destroy() {
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

  logRegisteredEndpoints() {
    if (!this.executor) return;
    const logger = this.executor.logger;
    logger.info("Kafka endpoints supported:");
    const regops = this.executor.getRegistrationsFor(this);
    regops.forEach((registeredOperation) => {
      const ro = registeredOperation.methodConfig as KafkaRegistrationInfo;
      if (ro.kafkaTopics) {
        const cname = registeredOperation.methodReg.className;
        const mname = registeredOperation.methodReg.name;
        if (Array.isArray(ro.kafkaTopics)) {
          ro.kafkaTopics.forEach( kafkaTopic => {
            logger.info(`    ${kafkaTopic} -> ${cname}.${mname}`);
          });
        } else {
          logger.info(`    ${ro.kafkaTopics} -> ${cname}.${mname}`);
        }
      }
    });
  }
}

/////////////////////////////
/* Kafka Method Decorators */
/////////////////////////////

let kafkaInst: DBOSKafka | undefined = undefined;

export interface KafkaRegistrationInfo {
  kafkaTopics?: string | RegExp | Array<string | RegExp>;
  consumerConfig?: ConsumerConfig;
  queueName?: string;
}

export function KafkaConsume(topics: string | RegExp | Array<string | RegExp>, consumerConfig?: ConsumerConfig, queueName ?: string) {
  function kafkadec<This, Ctx extends DBOSContext, Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: KafkaArgs) => Promise<Return>>
  ) {
    if (!kafkaInst) kafkaInst = new DBOSKafka();
    const {descriptor, receiverInfo} = associateMethodWithEventReceiver(kafkaInst, target, propertyKey, inDescriptor);

    const kafkaRegistration = receiverInfo as KafkaRegistrationInfo;
    kafkaRegistration.kafkaTopics = topics;
    kafkaRegistration.consumerConfig = consumerConfig;
    kafkaRegistration.queueName = queueName;

    return descriptor;
  }
  return kafkadec;
}

/////////////////////////////
/* Kafka Class Decorators  */
/////////////////////////////

export interface KafkaDefaults {
  kafkaConfig?: KafkaConfig;
}

export function Kafka(kafkaConfig: KafkaConfig) {
  function clsdec<T extends { new(...args: unknown[]): object }>(ctor: T) {
    if (!kafkaInst) kafkaInst = new DBOSKafka();
    const kafkaInfo = associateClassWithEventReceiver(kafkaInst, ctor) as KafkaDefaults;
    kafkaInfo.kafkaConfig = kafkaConfig;
  }
  return clsdec;
}
