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
  message: (functionName: string, topic: string, partition: number, message: KafkaMessage) => void;
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

  @kafkaReceiver.consumer('string-topic')
  @DBOS.workflow()
  static async stringTopic(topic: string, partition: number, message: KafkaMessage) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'stringTopic', topic, partition, message);
  }

  @kafkaReceiver.consumer(/regex-topic-.*/i)
  @DBOS.workflow()
  static async regexTopic(topic: string, partition: number, message: KafkaMessage) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'regexTopic', topic, partition, message);
  }

  @kafkaReceiver.consumer(['a-topic', 'b-topic'])
  @DBOS.workflow()
  static async stringArrayTopic(topic: string, partition: number, message: KafkaMessage) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'stringArrayTopic', topic, partition, message);
  }

  @kafkaReceiver.consumer([/z-topic-.*/i, /y-topic-.*/i])
  @DBOS.workflow()
  static async regexArrayTopic(topic: string, partition: number, message: KafkaMessage) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'regexArrayTopic', topic, partition, message);
  }

  static async registeredConsumer(topic: string, partition: number, message: KafkaMessage) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'registeredConsumer', topic, partition, message);
  }
}

KafkaTestClass.registeredConsumer = DBOS.registerWorkflow(KafkaTestClass.registeredConsumer, 'registeredConsumer');
kafkaReceiver.registerConsumer(KafkaTestClass.registeredConsumer, 'registered-topic');

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
suite('kafkajs-receive', async () => {
  const kafkaAvailable = await validateKafka(kafkaConfig);
  let producer: Producer | undefined = undefined;

  const testCases = [
    { topic: 'string-topic', functionName: 'stringTopic' },
    { topic: 'regex-topic-foo', functionName: 'regexTopic' },
    { topic: 'a-topic', functionName: 'stringArrayTopic' },
    { topic: 'b-topic', functionName: 'stringArrayTopic' },
    { topic: 'z-topic-foo', functionName: 'regexArrayTopic' },
    { topic: 'y-topic-foo', functionName: 'regexArrayTopic' },
    { topic: 'registered-topic', functionName: 'registeredConsumer' },
  ];

  before(
    async () => {
      if (!kafkaAvailable) {
        return;
      }

      const kafka = new Kafka(kafkaConfig);
      producer = kafka.producer();

      const topics = testCases.map((tc) => tc.topic);
      await Promise.all([setupTopics(kafka, topics), producer.connect()]);

      const client = new Client({ user: 'postgres', database: 'postgres' });
      try {
        await client.connect();
        Promise.all([dropDB(client, 'kafka_recv_test', true), dropDB(client, 'kafka_recv_test_dbos_sys', true)]);
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
