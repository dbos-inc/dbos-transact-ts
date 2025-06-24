import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka as KafkaJS, KafkaMessage, logLevel, Admin, KafkaConfig } from 'kafkajs';
import { KafkaReceiver } from '..';
import { EventEmitter } from 'node:events';

const kafkaConfig = {
  clientId: 'dbos-kafka-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  requestTimeout: 100,
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const kafkaReceiver = new KafkaReceiver(kafkaConfig);

interface KafkaEvents {
  message: (funcName: string, topic: string, partition: number, message: KafkaMessage) => void;
}

class KafkaEmitter extends EventEmitter {
  override on<K extends keyof KafkaEvents>(event: K, listener: KafkaEvents[K]): this {
    return super.on(event, listener);
  }

  override emit<K extends keyof KafkaEvents>(event: K, ...args: Parameters<KafkaEvents[K]>): boolean {
    return super.emit(event, ...args);
  }
}

type KafkaMessageEvent = {
  topic: string;
  partition: number;
  message: KafkaMessage;
};

function waitForMessage(emitter: KafkaEmitter, funcName: string, timeoutMS = 30000): Promise<KafkaMessageEvent> {
  return withTimeout(
    new Promise<KafkaMessageEvent>((resolve) => {
      const handler = (f: string, topic: string, partition: number, message: KafkaMessage) => {
        if (f === funcName) {
          emitter.off('message', handler);
          resolve({ topic, partition, message });
        }
      };
      emitter.on('message', handler);
    }),
    timeoutMS,
    `Timeout waiting for message for function ${funcName}`,
  );
}

class KafkaTestClass {
  static readonly emitter = new KafkaEmitter();

  @kafkaReceiver.eventConsumer('string-topic')
  @DBOS.workflow()
  static async stringTopic(topic: string, partition: number, message: KafkaMessage) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'stringTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer(/regex-topic-.*/i)
  @DBOS.workflow()
  static async regexTopic(topic: string, partition: number, message: KafkaMessage) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'regexTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer(['a-topic', 'b-topic'])
  @DBOS.workflow()
  static async stringArrayTopic(topic: string, partition: number, message: KafkaMessage) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'stringArrayTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer([/z-topic-.*/i, /y-topic-.*/i])
  @DBOS.workflow()
  static async regexArrayTopic(topic: string, partition: number, message: KafkaMessage) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'regexArrayTopic', topic, partition, message);
  }
}

async function ensureTopicsExist(kafkaConfig: KafkaConfig, topics: string[]) {
  const kafka = new KafkaJS(kafkaConfig);
  const admin = kafka.admin();
  try {
    const existingTopics = await admin.listTopics();
    const topicsToCreate = topics.filter((t) => !existingTopics.includes(t));
    if (topicsToCreate.length > 0) {
      await admin.createTopics({ topics: topicsToCreate.map((t) => ({ topic: t })) });
    }
    return true;
  } catch (e) {
    return false;
  } finally {
    await admin.disconnect();
  }
}

describe('kafkajs-receive', () => {
  let kafkaIsAvailable = false;

  beforeAll(async () => {
    // Create topics used in tests
    // As per https://kafka.js.org/docs/consuming:
    //    When supplying a regular expression, the consumer will not match topics created after the subscription.
    //    If your broker has topic-A and topic-B, you subscribe to /topic-.*/, then topic-C is created,
    //    your consumer would not be automatically subscribed to topic-C.
    const topics = ['string-topic', 'regex-topic-foo', 'a-topic', 'b-topic', 'z-topic-foo', 'y-topic-foo'];
    kafkaIsAvailable = await ensureTopicsExist(kafkaConfig, topics);

    const client = new Client({ user: 'postgres', database: 'postgres' });
    try {
      await client.connect();
      await dropDB(client, 'kafka_recv_test', true);
      await dropDB(client, 'kafka_recv_test_dbos_sys', true);
    } finally {
      await client.end();
    }
  }, 30000);

  beforeEach(async () => {
    DBOS.setConfig({ name: 'kafka-recv-test' });
    DBOS.registerLifecycleCallback(kafkaReceiver);
    await DBOS.launch();
  }, 30000);

  afterEach(async () => {
    await DBOS.shutdown();
  }, 30000);

  test('wf-string-topic', async () => {
    if (!kafkaIsAvailable) {
      DBOS.logger.warn('Kafka unavailable, skipping Kafka tests');
      return;
    }

    const topic = `string-topic`;
    const message = `test-message-${Date.now()}`;
    const kafka = new KafkaJS(kafkaConfig);
    const producer = kafka.producer();
    try {
      await producer.connect();
      await producer.send({ topic, messages: [{ value: message }] });
      const result = await waitForMessage(KafkaTestClass.emitter, 'stringTopic');
      expect(result.topic).toBe(topic);
      expect(String(result.message.value)).toBe(message);
    } finally {
      await producer.disconnect();
    }
  }, 40000);

  test('wf-regex-topic', async () => {
    if (!kafkaIsAvailable) {
      DBOS.logger.warn('Kafka unavailable, skipping Kafka tests');
      return;
    }

    const topic = `regex-topic-foo`;
    const message = `test-message-${Date.now()}`;
    const kafka = new KafkaJS(kafkaConfig);
    const producer = kafka.producer();
    try {
      await producer.connect();
      await producer.send({ topic, messages: [{ value: message }] });
      const result = await waitForMessage(KafkaTestClass.emitter, 'regexTopic');
      expect(result.topic).toBe(topic);
      expect(String(result.message.value)).toBe(message);
    } finally {
      await producer.disconnect();
    }
  }, 40000);

  test('wf-array-string-topic-a', async () => {
    if (!kafkaIsAvailable) {
      DBOS.logger.warn('Kafka unavailable, skipping Kafka tests');
      return;
    }

    const topic = `a-topic`;
    const message = `test-message-${Date.now()}`;
    const kafka = new KafkaJS(kafkaConfig);
    const producer = kafka.producer();
    try {
      await producer.connect();
      await producer.send({ topic, messages: [{ value: message }] });
      const result = await waitForMessage(KafkaTestClass.emitter, 'stringArrayTopic');
      expect(result.topic).toBe(topic);
      expect(String(result.message.value)).toBe(message);
    } finally {
      await producer.disconnect();
    }
  }, 40000);

  test('wf-array-string-topic-b', async () => {
    if (!kafkaIsAvailable) {
      DBOS.logger.warn('Kafka unavailable, skipping Kafka tests');
      return;
    }

    const topic = `b-topic`;
    const message = `test-message-${Date.now()}`;
    const kafka = new KafkaJS(kafkaConfig);
    const producer = kafka.producer();
    try {
      await producer.connect();
      await producer.send({ topic, messages: [{ value: message }] });
      const result = await waitForMessage(KafkaTestClass.emitter, 'stringArrayTopic');
      expect(result.topic).toBe(topic);
      expect(String(result.message.value)).toBe(message);
    } finally {
      await producer.disconnect();
    }
  }, 40000);

  test('wf-array-regex-topic-z', async () => {
    if (!kafkaIsAvailable) {
      DBOS.logger.warn('Kafka unavailable, skipping Kafka tests');
      return;
    }

    const topic = `z-topic-foo`;
    const message = `test-message-${Date.now()}`;
    const kafka = new KafkaJS(kafkaConfig);
    const producer = kafka.producer();
    try {
      await producer.connect();
      await producer.send({ topic, messages: [{ value: message }] });
      const result = await waitForMessage(KafkaTestClass.emitter, 'regexArrayTopic');
      expect(result.topic).toBe(topic);
      expect(String(result.message.value)).toBe(message);
    } finally {
      await producer.disconnect();
    }
  }, 40000);

  test('wf-array-regex-topic-y', async () => {
    if (!kafkaIsAvailable) {
      DBOS.logger.warn('Kafka unavailable, skipping Kafka tests');
      return;
    }

    const topic = `y-topic-foo`;
    const message = `test-message-${Date.now()}`;
    const kafka = new KafkaJS(kafkaConfig);
    const producer = kafka.producer();
    try {
      await producer.connect();
      await producer.send({ topic, messages: [{ value: message }] });
      const result = await waitForMessage(KafkaTestClass.emitter, 'regexArrayTopic');
      expect(result.topic).toBe(topic);
      expect(String(result.message.value)).toBe(message);
    } finally {
      await producer.disconnect();
    }
  }, 40000);
});
