import { DBOS, DBOSLifecycleCallback } from '@dbos-inc/dbos-sdk';

import { KafkaJS, LibrdKafkaError as KafkaError } from '@confluentinc/kafka-javascript';

type KafkaMessageHandler<Return> = (topic: string, partition: number, message: KafkaJS.Message) => Promise<Return>;

const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface KafkaMethodConfig {
  topics?: Array<string | RegExp>;
  config?: KafkaJS.ConsumerConstructorConfig;
  queueName?: string;
}

interface KafkaRetryConfig {
  maxRetries: number;
  retryTime: number;
  multiplier: number;
}

function safeGroupName(className: string, methodName: string, topics: Array<string | RegExp>) {
  const safeGroupIdPart = [className, methodName, ...topics]
    .map((r) => r.toString())
    .map((r) => r.replaceAll(/[^a-zA-Z0-9\\-]/g, ''))
    .join('-');
  return `dbos-kafka-group-${safeGroupIdPart}`.slice(0, 255);
}

function isKafkaError(e: unknown): e is KafkaError {
  if (e && typeof e === 'object') {
    return 'code' in e && typeof e.code === 'number';
  }
  return false;
}

export type ConsumerTopics = string | RegExp | Array<string | RegExp>;

export class ConfluentKafkaReceiver extends DBOSLifecycleCallback {
  readonly #consumers = new Array<KafkaJS.Consumer>();

  constructor(
    private readonly config: KafkaJS.KafkaConfig,
    private readonly retryConfig: KafkaRetryConfig = { maxRetries: 5, retryTime: 300, multiplier: 2 },
  ) {
    super();
  }

  override async initialize() {
    const { maxRetries, multiplier } = this.retryConfig;
    const clientId = this.config.clientId ?? 'dbos-confluent-kafka-receiver';
    const kafka = new KafkaJS.Kafka({ kafkaJS: { ...this.config, clientId } });

    for (const regOp of DBOS.getAssociatedInfo(this)) {
      const func = regOp.methodReg.registeredFunction as KafkaMessageHandler<unknown> | undefined;
      if (func === undefined) {
        continue; // TODO: Log?
      }

      const methodConfig = regOp.methodConfig as KafkaMethodConfig;
      const topics = methodConfig.topics ?? [];
      if (topics.length === 0) {
        continue; // TODO: Log?
      }

      const { name, className } = regOp.methodReg;
      const config: KafkaJS.ConsumerConstructorConfig = methodConfig.config ?? {
        'group.id': safeGroupName(className, name, topics),
      };
      const consumer = kafka.consumer({ ...config, 'auto.offset.reset': 'earliest' });
      await consumer.connect();

      // A temporary workaround for https://github.com/tulios/kafkajs/pull/1558 until it gets fixed
      // If topic auto-creation is on and you try to subscribe to a nonexistent topic, KafkaJS should retry until the topic is created.
      // However, it has a bug where it won't. Thus, we retry instead.
      let { retryTime } = this.retryConfig;
      for (let i = 1; i <= maxRetries; i++) {
        try {
          await consumer.subscribe({ topics });
          break;
        } catch (e) {
          if (isKafkaError(e) && e.code === 3 && i < maxRetries) {
            await sleepms(retryTime);
            retryTime *= multiplier;
          } else {
            throw e;
          }
        }
      }

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          DBOS.logger.debug(
            `ConfluentKafkaReceiver message on topic ${topic} partition ${partition} offset ${message.offset}`,
          );
          try {
            const workflowID = `confluent-kafka-${topic}-${partition}-${config['group.id']}-${message.offset}`;
            const wfParams = { workflowID, queueName: methodConfig.queueName };
            await DBOS.startWorkflow(func, wfParams)(topic, partition, message);
          } catch (e) {
            const message = e instanceof Error ? e.message : String(e);
            DBOS.logger.error(`Error processing Kafka message ${message}`);
            throw e;
          }
        },
      });

      this.#consumers.push(consumer);
    }
  }

  override async destroy() {
    const disconnectPromises = this.#consumers.splice(0, this.#consumers.length).map((c) => c.disconnect());
    await Promise.allSettled(disconnectPromises);
  }

  override logRegisteredEndpoints() {
    DBOS.logger.info('KafkaJS receiver endpoints:');

    const regOps = DBOS.getAssociatedInfo(this);
    for (const regOp of regOps) {
      const methodConfig = regOp.methodConfig as KafkaMethodConfig;
      const { name, className } = regOp.methodReg;
      for (const topic of methodConfig.topics ?? []) {
        DBOS.logger.info(`    ${topic} -> ${className}.${name}`);
      }
    }
  }

  registerConsumer<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    topics: ConsumerTopics,
    options: {
      classOrInst?: object;
      className?: string;
      name?: string;
      queueName?: string;
      config?: KafkaJS.ConsumerConstructorConfig;
    } = {},
  ) {
    const { regInfo } = DBOS.associateFunctionWithInfo(this, func, {
      classOrInst: options.classOrInst,
      className: options.className,
      name: options.name ?? func.name,
    });

    const kafkaRegInfo = regInfo as KafkaMethodConfig;
    kafkaRegInfo.topics = Array.isArray(topics) ? topics : [topics];
    kafkaRegInfo.queueName = options.queueName;
    kafkaRegInfo.config = options.config;
  }

  consumer(topics: ConsumerTopics, options: { queueName?: string; config?: KafkaJS.ConsumerConstructorConfig } = {}) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const $this = this;
    function methodDecorator<This, Args extends [string, number, KafkaJS.Message], Return>(
      target: object,
      propertyKey: PropertyKey,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      if (descriptor.value) {
        $this.registerConsumer(descriptor.value, topics, {
          classOrInst: target,
          name: String(propertyKey),
          queueName: options.queueName,
          config: options.config,
        });
      }
      return descriptor;
    }
    return methodDecorator;
  }
}
