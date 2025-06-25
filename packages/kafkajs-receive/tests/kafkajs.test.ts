import { after, afterEach, before, beforeEach, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaMessage, logLevel, KafkaConfig, Producer } from 'kafkajs';
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
    DBOS.logger.info(`KafkaEmitter topic ${args[1]} partition ${args[2]} offset ${args[3].offset}`);

    return super.emit(event, ...args);
  }
}

type KafkaMessageEvent = {
  topic: string;
  partition: number;
  message: KafkaMessage;
};

function waitForMessage(
  emitter: KafkaEmitter,
  funcName: string,
  topic: string,
  timeoutMS = 45000,
): Promise<KafkaMessageEvent> {
  return withTimeout(
    new Promise<KafkaMessageEvent>((resolve) => {
      const handler = (f: string, t: string, partition: number, message: KafkaMessage) => {
        if (f === funcName && t === topic) {
          emitter.off('message', handler);
          resolve({ topic: t, partition, message });
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

async function validateKafka(config: KafkaConfig) {
  const kafka = new Kafka(config);
  const admin = kafka.admin();
  try {
    await admin.connect();
    await admin.listTopics();
    return true;
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    DBOS.logger.error(message);
    return false;
  } finally {
    await admin.disconnect();
  }
}

async function setupTopics(kafka: Kafka, topics: string[]) {
  const admin = kafka.admin();
  try {
    await admin.connect();
    // Delete and recreate topics to ensure a clean state
    const existingTopics = await admin.listTopics();
    const topicsToDelete = topics.filter((t) => existingTopics.includes(t));
    await admin.deleteTopics({ topics: topicsToDelete, timeout: 5000 });

    await new Promise((r) => setTimeout(r, 3000));
    await admin.createTopics({
      topics: topics.map((t) => ({
        topic: t,
        numPartitions: 1,
        replicationFactor: 1,
      })),
      timeout: 5000,
    });
  } finally {
    await admin.disconnect();
  }
}

suite('kafkajs-receive', async () => {
  const kafkaAvailable = await validateKafka(kafkaConfig);
  let producer: Producer | undefined = undefined;

  before(
    async () => {
      if (!kafkaAvailable) {
        return;
      }

      const kafka = new Kafka(kafkaConfig);

      // Create topics used in tests
      // As per https://kafka.js.org/docs/consuming:
      //    When supplying a regular expression, the consumer will not match topics created after the subscription.
      //    If your broker has topic-A and topic-B, you subscribe to /topic-.*/, then topic-C is created,
      //    your consumer would not be automatically subscribed to topic-C.
      const topics = ['string-topic', 'regex-topic-foo', 'a-topic', 'b-topic', 'z-topic-foo', 'y-topic-foo'];
      await setupTopics(kafka, topics);

      producer = kafka.producer();
      await producer.connect();

      const client = new Client({ user: 'postgres', database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'kafka_recv_test', true);
        await dropDB(client, 'kafka_recv_test_dbos_sys', true);
      } finally {
        await client.end();
      }
    },
    { timeout: 30000 },
  );

  after(
    async () => {
      await producer?.disconnect();
    },
    { timeout: 30000 },
  );

  beforeEach(async () => {
    if (producer) {
      DBOS.setConfig({ name: 'kafka-recv-test' });
      DBOS.registerLifecycleCallback(kafkaReceiver);
      await DBOS.launch();
    }
  });

  afterEach(
    async () => {
      if (producer) {
        await DBOS.shutdown();
      }
    },
    { timeout: 30000 },
  );

  test('wf-string-topic', { skip: !kafkaAvailable, timeout: 40000 }, async () => {
    const topic = `string-topic`;
    const message = `test-message-${Date.now()}`;
    await producer!.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'stringTopic', topic);

    assert.equal(topic, result.topic);
    assert.equal(message, String(result.message.value));
  });

  test('wf-regex-topic', { skip: !kafkaAvailable, timeout: 40000 }, async () => {
    const topic = `regex-topic-foo`;
    const message = `test-message-${Date.now()}`;
    await producer!.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'regexTopic', topic);

    assert.equal(topic, result.topic);
    assert.equal(message, String(result.message.value));
  });

  test('wf-array-string-topic-a', { skip: !kafkaAvailable, timeout: 40000 }, async () => {
    const topic = `a-topic`;
    const message = `test-message-${Date.now()}`;
    await producer!.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'stringArrayTopic', topic);

    assert.equal(topic, result.topic);
    assert.equal(message, String(result.message.value));
  });

  test('wf-array-string-topic-b', { skip: !kafkaAvailable, timeout: 40000 }, async () => {
    const topic = `b-topic`;
    const message = `test-message-${Date.now()}`;
    await producer!.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'stringArrayTopic', topic);

    assert.equal(topic, result.topic);
    assert.equal(message, String(result.message.value));
  });

  test('wf-array-regex-topic-z', { skip: !kafkaAvailable, timeout: 40000 }, async () => {
    const topic = `z-topic-foo`;
    const message = `test-message-${Date.now()}`;
    await producer!.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'regexArrayTopic', topic);

    assert.equal(topic, result.topic);
    assert.equal(message, String(result.message.value));
  });

  test('wf-array-regex-topic-y', { skip: !kafkaAvailable, timeout: 40000 }, async () => {
    const topic = `y-topic-foo`;
    const message = `test-message-${Date.now()}`;
    await producer!.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'regexArrayTopic', topic);

    assert.equal(topic, result.topic);
    assert.equal(message, String(result.message.value));
  });
});
