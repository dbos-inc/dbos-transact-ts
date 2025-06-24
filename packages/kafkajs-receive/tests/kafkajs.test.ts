import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import {
  Kafka as KafkaJS,
  Consumer,
  ConsumerConfig,
  KafkaConfig,
  KafkaMessage,
  KafkaJSProtocolError,
  logLevel,
} from 'kafkajs';
import { KafkaReceiver } from '..';
import { EventEmitter } from 'node:events';

const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

const kafkaConfig = {
  clientId: 'dbos-kafka-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  requestTimeout: 100,
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const kafkaReceiver = new KafkaReceiver(kafkaConfig);

interface KafkaEvents {
  message: (func: string, topic: string, partition: number, message: KafkaMessage) => void;
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

function waitForMessage(emitter: KafkaEmitter, func: string, timeoutMS = 30000): Promise<KafkaMessageEvent> {
  return withTimeout(
    new Promise<KafkaMessageEvent>((resolve) => {
      const handler = (f: string, topic: string, partition: number, message: KafkaMessage) => {
        if (f === func) {
          emitter.off('message', handler);
          resolve({ topic, partition, message });
        }
      };
      emitter.on('message', handler);
    }),
    timeoutMS,
    `Timeout waiting for message for function ${func}`,
  );
}

class KafkaTestClass {
  static readonly emitter = new KafkaEmitter();

  @kafkaReceiver.eventConsumer('string-topic')
  @DBOS.workflow()
  static async stringTopic(topic: string, partition: number, message: KafkaMessage) {
    KafkaTestClass.emitter.emit('message', 'stringTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer(new RegExp(/regex-topic-.*/))
  @DBOS.workflow()
  static async regexTopic(topic: string, partition: number, message: KafkaMessage) {
    KafkaTestClass.emitter.emit('message', 'regexTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer(['a-topic', 'b-topic'])
  @DBOS.workflow()
  static async stringArrayTopic(topic: string, partition: number, message: KafkaMessage) {
    KafkaTestClass.emitter.emit('message', 'stringArrayTopic', topic, partition, message);
  }
}

async function verifyKafka(config: KafkaConfig) {
  const kafka = new KafkaJS(kafkaConfig);
  const admin = kafka.admin();
  try {
    await admin.connect();
    const _topics = await admin.listTopics();
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
    kafkaIsAvailable = await verifyKafka(kafkaConfig);

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

  test.skip('wf-regex-topic', async () => {
    if (!kafkaIsAvailable) {
      DBOS.logger.warn('Kafka unavailable, skipping Kafka tests');
      return;
    }

    const topic = `regex-topic-${Date.now()}`;
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
});
