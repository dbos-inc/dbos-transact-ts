import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { ConfluentKafkaReceiver } from '..';
import { EventEmitter } from 'node:events';
import { KafkaJS as ConfluentKafkaJS } from '@confluentinc/kafka-javascript';
import { KafkaConfig, Kafka as KafkaJS } from 'kafkajs';

const kafkaConfig = {
  clientId: 'dbos-kafka-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  requestTimeout: 100,
  retry: { retries: 5 },
  logLevel: 0,
};

const kafkaReceiver = new ConfluentKafkaReceiver(kafkaConfig);

interface KafkaEvents {
  message: (funcName: string, topic: string, partition: number, message: ConfluentKafkaJS.Message) => void;
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
    DBOS.logger.warn(`stringTopic received message on topic ${topic}`);
    KafkaTestClass.emitter.emit('message', 'stringTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer(/^regex-topic-.*/)
  @DBOS.workflow()
  static async regexTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    DBOS.logger.warn(`regexTopic received message on topic ${topic}`);
    KafkaTestClass.emitter.emit('message', 'regexTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer(['a-topic', 'b-topic'])
  @DBOS.workflow()
  static async stringArrayTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    DBOS.logger.warn(`stringArrayTopic received message on topic ${topic}`);
    KafkaTestClass.emitter.emit('message', 'stringArrayTopic', topic, partition, message);
  }

  @kafkaReceiver.eventConsumer([/^z-topic-.*/, /^y-topic-.*/])
  @DBOS.workflow()
  static async regexArrayTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    DBOS.logger.warn(`regexArrayTopic received message on topic ${topic}`);
    KafkaTestClass.emitter.emit('message', 'regexArrayTopic', topic, partition, message);
  }
}

async function setupTopics(config: KafkaConfig, topics: string[]) {
  const kafka = new KafkaJS(config);
  const admin = kafka.admin();
  try {
    await admin.connect();

    // Delete and recreate topics to ensure a clean state
    await admin.deleteTopics({ topics, timeout: 5000 });
    await new Promise((r) => setTimeout(r, 3000));
    await admin.createTopics({
      topics: topics.map((t) => ({
        topic: t,
        numPartitions: 1,
        replicationFactor: 1,
      })),
      timeout: 5000,
    });
    return true;
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    DBOS.logger.error(message);
    return false;
  } finally {
    await admin.disconnect();
  }
}

describe.skip('confluent-kafka-receive', () => {
  let producer: ConfluentKafkaJS.Producer | undefined = undefined;

  beforeAll(async () => {
    const topics = ['string-topic', 'regex-topic-foo', 'a-topic', 'b-topic', 'z-topic-foo', 'y-topic-foo'];
    const kafkaAvailable = await setupTopics(kafkaConfig, topics);

    if (!kafkaAvailable) {
      return;
    }

    const kafka = new ConfluentKafkaJS.Kafka({ kafkaJS: kafkaConfig });
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
  }, 30000);

  afterAll(async () => {
    await producer?.disconnect();
  }, 30000);

  beforeEach(async () => {
    if (producer) {
      DBOS.setConfig({ name: 'conf-kafka-recv-test' });
      DBOS.registerLifecycleCallback(kafkaReceiver);
      await DBOS.launch();
    }
  }, 30000);

  afterEach(async () => {
    if (producer) {
      await DBOS.shutdown();
    }
  }, 30000);

  test('wf-string-topic', async () => {
    if (!producer) {
      DBOS.logger.warn('Skipping test, producer not initialized');
      return;
    }
    const topic = `string-topic`;
    const message = `test-message-${Date.now()}`;
    await producer.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'stringTopic', topic);
    expect(result.topic).toBe(topic);
    expect(String(result.message.value)).toBe(message);
  }, 50000);

  test('wf-regex-topic', async () => {
    if (!producer) {
      DBOS.logger.warn('Skipping test, producer not initialized');
      return;
    }
    const topic = `regex-topic-foo`;
    const message = `test-message-${Date.now()}`;
    await producer.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'regexTopic', topic);
    expect(result.topic).toBe(topic);
    expect(String(result.message.value)).toBe(message);
  }, 50000);

  test('wf-array-string-topic-a', async () => {
    if (!producer) {
      DBOS.logger.warn('Skipping test, producer not initialized');
      return;
    }
    const topic = `a-topic`;
    const message = `test-message-${Date.now()}`;
    await producer.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'stringArrayTopic', topic);
    expect(result.topic).toBe(topic);
    expect(String(result.message.value)).toBe(message);
  }, 50000);

  test('wf-array-string-topic-b', async () => {
    if (!producer) {
      DBOS.logger.warn('Skipping test, producer not initialized');
      return;
    }
    const topic = `b-topic`;
    const message = `test-message-${Date.now()}`;
    await producer.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'stringArrayTopic', topic);
    expect(result.topic).toBe(topic);
    expect(String(result.message.value)).toBe(message);
  }, 50000);

  test('wf-array-regex-topic-z', async () => {
    if (!producer) {
      DBOS.logger.warn('Skipping test, producer not initialized');
      return;
    }
    const topic = `z-topic-foo`;
    const message = `test-message-${Date.now()}`;
    await producer.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'regexArrayTopic', topic);
    expect(result.topic).toBe(topic);
    expect(String(result.message.value)).toBe(message);
  }, 50000);

  test('wf-array-regex-topic-y', async () => {
    if (!producer) {
      DBOS.logger.warn('Skipping test, producer not initialized');
      return;
    }
    const topic = `y-topic-foo`;
    const message = `test-message-${Date.now()}`;
    await producer.send({ topic, messages: [{ value: message }] });
    const result = await waitForMessage(KafkaTestClass.emitter, 'regexArrayTopic', topic);
    expect(result.topic).toBe(topic);
    expect(String(result.message.value)).toBe(message);
  }, 50000);
});
