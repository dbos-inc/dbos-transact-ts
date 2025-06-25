import { after, afterEach, before, beforeEach, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, Producer } from 'kafkajs';
import { EventEmitter } from 'node:events';
import { ConfluentKafkaReceiver } from '..';
import { KafkaJS as ConfluentKafkaJS } from '@confluentinc/kafka-javascript';

const kafkaConfig = {
  clientId: 'dbos-kafka-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  requestTimeout: 100,
  retry: { retries: 5 },
  logLevel: 0,
};

const kafkaReceiver = new ConfluentKafkaReceiver(kafkaConfig);

interface KafkaEvents {
  message: (functionName: string, topic: string, partition: number, message: ConfluentKafkaJS.Message) => void;
}

class KafkaEmitter extends EventEmitter {
  override on<K extends keyof KafkaEvents>(event: K, listener: KafkaEvents[K]): this {
    return super.on(event, listener);
  }

  override emit<K extends keyof KafkaEvents>(event: K, ...args: Parameters<KafkaEvents[K]>): boolean {
    DBOS.logger.info(`KafkaEmitter topic ${args[1]} partition ${args[2]}`);
    return super.emit(event, ...args);
  }
}

type KafkaMessageEvent = {
  topic: string;
  partition: number;
  message: ConfluentKafkaJS.Message;
};

function waitForMessage(
  emitter: KafkaEmitter,
  funcName: string,
  topic: string,
  timeoutMS = 45000,
): Promise<KafkaMessageEvent> {
  return withTimeout(
    new Promise<KafkaMessageEvent>((resolve) => {
      const handler = (f: string, t: string, partition: number, message: ConfluentKafkaJS.Message) => {
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
  static async stringTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'stringTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer(/^regex-topic-.*/)
  @DBOS.workflow()
  static async regexTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'regexTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer(['a-topic', 'b-topic'])
  @DBOS.workflow()
  static async stringArrayTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'stringArrayTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer([/^z-topic-.*/, /^y-topic-.*/])
  @DBOS.workflow()
  static async regexArrayTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
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

// eslint-disable-next-line @typescript-eslint/no-floating-promises
suite('confluent-kafka-receive', async () => {
  const kafkaAvailable = await validateKafka(kafkaConfig);
  let producer: Producer | undefined = undefined;

  const testCases = [
    { topic: 'string-topic', functionName: 'stringTopic' },
    { topic: 'regex-topic-foo', functionName: 'regexTopic' },
    { topic: 'a-topic', functionName: 'stringArrayTopic' },
    { topic: 'b-topic', functionName: 'stringArrayTopic' },
    { topic: 'z-topic-foo', functionName: 'regexArrayTopic' },
    { topic: 'y-topic-foo', functionName: 'regexArrayTopic' },
  ];

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
      const topics = testCases.map((tc) => tc.topic);
      await setupTopics(kafka, topics);

      producer = kafka.producer();
      await producer.connect();

      const client = new Client({ user: 'postgres', database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'conf_kafka_recv_test', true);
        await dropDB(client, 'conf_kafka_recv_test_dbos_sys', true);
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
      DBOS.setConfig({ name: 'conf-kafka-recv-test' });
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

  for (const { functionName, topic } of testCases) {
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    test(`${topic}-${functionName}`, { skip: !kafkaAvailable, timeout: 40000 }, async () => {
      const message = `test-message-${Date.now()}`;
      await producer!.send({ topic, messages: [{ value: message }] });
      const result = await waitForMessage(KafkaTestClass.emitter, functionName, topic);

      assert.equal(topic, result.topic);
      assert.equal(message, String(result.message.value));
    });
  }
});
