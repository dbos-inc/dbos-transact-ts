import {
  Kafka as KafkaJS,
  Consumer,
  ConsumerConfig,
  Producer,
  ProducerConfig,
  KafkaConfig,
  KafkaMessage,
  Message,
  KafkaJSProtocolError,
  logLevel,
  Partitioners,
} from 'kafkajs';
import { DBOS, Step, StepContext, ConfiguredInstance, DBOSContext, DBOSEventReceiver } from '@dbos-inc/dbos-sdk';
import { associateClassWithEventReceiver, associateMethodWithEventReceiver } from '@dbos-inc/dbos-sdk';
import { TransactionFunction } from '@dbos-inc/dbos-sdk';
import { WorkflowFunction } from '@dbos-inc/dbos-sdk';
import { Error as DBOSError } from '@dbos-inc/dbos-sdk';
import { DBOSExecutorContext } from '@dbos-inc/dbos-sdk';

export {
  KafkaConfig,
  ConsumerConfig,
  ProducerConfig,
  KafkaMessage,
  KafkaJSProtocolError as KafkaError,
  logLevel,
  Partitioners,
};

type KafkaArgs = [string, number, KafkaMessage];

const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

////////////////////////
/* Kafka Management  */
///////////////////////

export class DBOSKafka implements DBOSEventReceiver {
  readonly consumers: Consumer[] = [];
  kafka?: KafkaJS = undefined;

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
        if (!method.txnConfig && !method.workflowConfig) {
          throw new DBOSError.DBOSError(
            `Error registering method ${cname}.${mname}: A Kafka decorator can only be assigned to a transaction or workflow!`,
          );
        }
        if (!defaults.kafkaConfig) {
          throw new DBOSError.DBOSError(
            `Error registering method ${cname}.${mname}: Kafka configuration not found. Does class ${cname} have an @Kafka decorator?`,
          );
        }
        const topics: Array<string | RegExp> = [];
        if (Array.isArray(ro.kafkaTopics)) {
          topics.push(...ro.kafkaTopics);
        } else if (ro.kafkaTopics) {
          topics.push(ro.kafkaTopics);
        }
        if (!this.kafka) {
          this.kafka = new KafkaJS(defaults.kafkaConfig);
        }
        const consumerConfig = ro.consumerConfig ?? { groupId: `${this.safeGroupName(cname, mname, topics)}` };
        const consumer = this.kafka.consumer(consumerConfig);
        await consumer.connect();
        // A temporary workaround for https://github.com/tulios/kafkajs/pull/1558 until it gets fixed
        // If topic autocreation is on and you try to subscribe to a nonexistent topic, KafkaJS should retry until the topic is created.
        // However, it has a bug where it won't. Thus, we retry instead.
        const maxRetries = defaults.kafkaConfig.retry ? (defaults.kafkaConfig.retry.retries ?? 5) : 5;
        let retryTime = defaults.kafkaConfig.retry ? (defaults.kafkaConfig.retry.maxRetryTime ?? 300) : 300;
        const multiplier = defaults.kafkaConfig.retry ? (defaults.kafkaConfig.retry.multiplier ?? 2) : 2;
        for (let i = 0; i < maxRetries; i++) {
          try {
            await consumer.subscribe({ topics: topics, fromBeginning: true });
            break;
          } catch (error) {
            const e = error as KafkaJSProtocolError;
            if (e.code === 3 && i + 1 < maxRetries) {
              // UNKNOWN_TOPIC_OR_PARTITION
              await sleepms(retryTime);
              retryTime *= multiplier;
              continue;
            } else {
              throw e;
            }
          }
        }
        await consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            const logger = this.executor!.logger;
            try {
              // This combination uniquely identifies a message for a given Kafka cluster
              const workflowUUID = `kafka-unique-id-${topic}-${partition}-${consumerConfig.groupId}-${message.offset}`;
              const wfParams = { workflowUUID: workflowUUID, configuredInstance: null, queueName: ro.queueName };
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

let kafkaInst: DBOSKafka | undefined = undefined;

export interface KafkaRegistrationInfo {
  kafkaTopics?: string | RegExp | Array<string | RegExp>;
  consumerConfig?: ConsumerConfig;
  queueName?: string;
}

export function KafkaConsume(
  topics: string | RegExp | Array<string | RegExp>,
  consumerConfig?: ConsumerConfig,
  queueName?: string,
) {
  function kafkadec<This, Ctx extends DBOSContext, Args extends KafkaArgs | [Ctx, ...KafkaArgs], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  ) {
    if (!kafkaInst) kafkaInst = new DBOSKafka();
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

export function Kafka(kafkaConfig: KafkaConfig) {
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    if (!kafkaInst) kafkaInst = new DBOSKafka();
    const kafkaInfo = associateClassWithEventReceiver(kafkaInst, ctor) as KafkaDefaults;
    kafkaInfo.kafkaConfig = kafkaConfig;
  }
  return clsdec;
}

//////////////////////////////
/* Producer Step    */
//////////////////////////////
export class KafkaProduceStep extends ConfiguredInstance {
  producer: Producer | undefined = undefined;
  topic: string = '';

  constructor(
    name: string,
    readonly cfg: KafkaConfig,
    topic: string,
    readonly pcfg?: ProducerConfig,
  ) {
    super(name);
    this.topic = topic;
  }

  override async initialize(): Promise<void> {
    const kafka = new KafkaJS(this.cfg);
    this.producer = kafka.producer(this.pcfg);
    await this.producer.connect();
    return Promise.resolve();
  }

  @Step()
  async sendMessage(_ctx: StepContext, msg: Message) {
    return await this.producer?.send({ topic: this.topic, messages: [msg] });
  }

  @Step()
  async sendMessages(_ctx: StepContext, msg: Message[]) {
    return await this.producer?.send({ topic: this.topic, messages: msg });
  }

  @DBOS.step()
  async send(msg: Message | Message[]) {
    return await this.producer?.send({ topic: this.topic, messages: msg instanceof Array ? msg : [msg] });
  }

  async disconnect() {
    await this.producer?.disconnect();
  }
}
