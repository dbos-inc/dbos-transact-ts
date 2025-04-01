import {
  DBOS,
  ConfiguredInstance,
  Error as DBOSError,
  DBOSEventReceiver,
  DBOSExecutorContext,
  WorkflowFunction,
  TransactionFunction,
  associateClassWithEventReceiver,
  associateMethodWithEventReceiver,
} from '@dbos-inc/dbos-sdk';

import { KafkaJS, LibrdKafkaError as KafkaError } from '@confluentinc/kafka-javascript';

export type KafkaConfig = KafkaJS.KafkaConfig;
export const logLevel = KafkaJS.logLevel;
export type Message = KafkaJS.Message;

const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

type KafkaArgs = [string, number, KafkaJS.KafkaMessage];

////////////////////////
/* Kafka Management  */
///////////////////////

export class DBOSConfluentKafka implements DBOSEventReceiver {
  readonly consumers: KafkaJS.Consumer[] = [];

  executor?: DBOSExecutorContext = undefined;

  constructor() {}

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
        console.log(`Registering ${cname}.${mname}:${JSON.stringify(ro.kafkaTopics)}`);
        if (!method.txnConfig && !method.workflowConfig) {
          throw new DBOSError.DBOSError(
            `Error registering method ${cname}.${mname}: A CKafka decorator can only be assigned to a transaction or workflow.`,
          );
        }
        if (!defaults.kafkaConfig) {
          throw new DBOSError.DBOSError(
            `Error registering method ${cname}.${mname}: Kafka configuration not found.  (Does class ${cname} have an @CKafka decorator?)`,
          );
        }
        const topics: Array<string | RegExp> = [];
        if (Array.isArray(ro.kafkaTopics)) {
          topics.push(...ro.kafkaTopics);
        } else if (ro.kafkaTopics) {
          topics.push(ro.kafkaTopics);
        }
        let clientId = defaults.kafkaConfig.clientId ?? 'dbos-kafka';
        clientId = clientId + `${cname}-${mname}`;
        const kafka = new KafkaJS.Kafka({ kafkaJS: { ...defaults.kafkaConfig, clientId: clientId } });
        const consumerConfig = ro.consumerConfig
          ? { ...ro.consumerConfig, 'auto.offset.reset': 'earliest' }
          : { 'group.id': `${this.safeGroupName(cname, mname, topics)}`, 'auto.offset.reset': 'earliest' };
        const consumer = kafka.consumer(consumerConfig);
        await consumer.connect();
        // Unclear if we need this:
        // A temporary workaround for https://github.com/tulios/kafkajs/pull/1558 until it gets fixed
        // If topic autocreation is on and you try to subscribe to a nonexistent topic, KafkaJS should retry until the topic is created.
        // However, it has a bug where it won't. Thus, we retry instead.
        const maxRetries = defaults.kafkaConfig.retry ? (defaults.kafkaConfig.retry.retries ?? 5) : 5;
        let retryTime = defaults.kafkaConfig.retry ? (defaults.kafkaConfig.retry.maxRetryTime ?? 300) : 300;
        const multiplier = /* defaults.kafkaConfig.retry ? defaults.kafkaConfig.retry.multiplier ?? 2 :*/ 2;
        for (let i = 0; i < maxRetries; i++) {
          try {
            console.log(`Subscribe to ${JSON.stringify(topics)}`);
            await consumer.subscribe({ topics: topics });
            break;
          } catch (error) {
            const e = error as KafkaError;
            if (e.code === 3 && i + 1 < maxRetries) {
              // UNKNOWN_TOPIC_OR_PARTITION
              await sleepms(retryTime);
              retryTime *= multiplier;
              continue;
            } else {
              throw error;
            }
          }
        }
        await consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            const logger = this.executor!.logger;
            try {
              // This combination uniquely identifies a message for a given Kafka cluster (and should ID the workflow also)
              const workflowID = `kafka-unique-id-${topic}-${partition}-${consumerConfig['group.id']}-${message.offset}`;
              const wfParams = { workflowUUID: workflowID, configuredInstance: null, queueName: ro.queueName };
              // All operations annotated with Kafka decorators must take in these three arguments
              const args: KafkaArgs = [topic, partition, message];
              // We can only guarantee exactly-once-per-message execution of transactions and workflows.
              if (method.txnConfig) {
                // Execute the transaction
                await this.executor!.transaction(
                  method.registeredFunction as TransactionFunction<unknown[], unknown>,
                  wfParams,
                  ...args,
                );
              } else if (method.workflowConfig) {
                // Safely start the workflow
                await this.executor!.workflow(
                  method.registeredFunction as unknown as WorkflowFunction<unknown[], unknown>,
                  wfParams,
                  ...args,
                );
              }
            } catch (e) {
              const error = e as Error;
              logger.error(`Error processing Kafka message: ${error.message}`);
              throw error;
            }
          },
        });
        this.consumers.push(consumer);
      }
    }
  }

  async destroy() {
    for (const consumer of this.consumers) {
      //await consumer.stop();
      await consumer.disconnect();
    }
  }

  safeGroupName(cls: string, func: string, topics: Array<string | RegExp>) {
    const safeGroupIdPart = [cls, func, ...topics]
      .map((r) => r.toString())
      .map((r) => r.replaceAll(/[^a-zA-Z0-9\\-]/g, ''))
      .join('-');
    return `dbos-kafka-group-${safeGroupIdPart}`.slice(0, 255);
  }

  logRegisteredEndpoints() {
    if (!this.executor) return;
    const logger = this.executor.logger;
    logger.info('Kafka endpoints supported:');
    const regops = this.executor.getRegistrationsFor(this);
    regops.forEach((registeredOperation) => {
      const ro = registeredOperation.methodConfig as KafkaRegistrationInfo;
      if (ro.kafkaTopics) {
        const cname = registeredOperation.methodReg.className;
        const mname = registeredOperation.methodReg.name;
        if (Array.isArray(ro.kafkaTopics)) {
          ro.kafkaTopics.forEach((kafkaTopic) => {
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

let kafkaInst: DBOSConfluentKafka | undefined = undefined;

export interface KafkaRegistrationInfo {
  kafkaTopics?: string | RegExp | Array<string | RegExp>;
  consumerConfig?: KafkaConfig;
  queueName?: string;
}

export function CKafkaConsume(
  topics: string | RegExp | Array<string | RegExp>,
  consumerConfig?: KafkaConfig,
  queueName?: string,
) {
  function kafkadec<This, Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: KafkaArgs) => Promise<Return>>,
  ) {
    if (!kafkaInst) kafkaInst = new DBOSConfluentKafka();
    const { descriptor, receiverInfo } = associateMethodWithEventReceiver(kafkaInst, target, propertyKey, inDescriptor);

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

export function CKafka(kafkaConfig: KafkaConfig) {
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    if (!kafkaInst) kafkaInst = new DBOSConfluentKafka();
    const kafkaInfo = associateClassWithEventReceiver(kafkaInst, ctor) as KafkaDefaults;
    kafkaInfo.kafkaConfig = kafkaConfig;
  }
  return clsdec;
}

//////////////////////////////
/* Kafka Producer           */
//////////////////////////////
export class KafkaProducer extends ConfiguredInstance {
  producer: KafkaJS.Producer | undefined = undefined;
  topic: string = '';

  constructor(
    name: string,
    readonly cfg: KafkaJS.ProducerConfig,
    topic: string,
  ) {
    super(name);
    this.topic = topic;
  }

  override async initialize(): Promise<void> {
    const kafka = new KafkaJS.Kafka({});
    this.producer = kafka.producer({ kafkaJS: this.cfg });
    await this.producer.connect();
  }

  @DBOS.step()
  async sendMessage(msg: KafkaJS.Message) {
    return await this.producer?.send({ topic: this.topic, messages: [msg] });
  }

  @DBOS.step()
  async sendMessages(msg: KafkaJS.Message[]) {
    return await this.producer?.send({ topic: this.topic, messages: msg });
  }

  async disconnect() {
    await this.producer?.disconnect();
  }
}
