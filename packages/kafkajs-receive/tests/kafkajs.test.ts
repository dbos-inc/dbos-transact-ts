import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB } from './test-helpers';
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

const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

const kafkaConfig = {
  clientId: 'dbos-kafka-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  requestTimeout: 100,
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const kafkaReceiver = new KafkaReceiver(kafkaConfig);

class KafkaTestClass {
  @kafkaReceiver.eventConsumer('test-topic')
  @DBOS.workflow()
  async handleMessage(topic: string, partition: number, message: KafkaMessage) {
    DBOS.logger.info(`Received message ${DBOS.workflowID}`);
    DBOS.logger.info(`Received message on topic ${topic}, partition ${partition}: ${message.value}`);
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
  });

  beforeEach(async () => {
    DBOS.setConfig({ name: 'kafka-recv-test' });
    DBOS.registerLifecycleCallback(kafkaReceiver);

    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('can initialize and shutdown', async () => {
    if (!kafkaIsAvailable) {
      DBOS.logger.warn('Kafka unavailable, skipping Kafka tests');
      return;
    }

    const kafka = new KafkaJS(kafkaConfig);
    const producer = kafka.producer();
    try {
      await producer.connect();
      producer.send({ topic: 'test-topic', messages: [{ value: 'test-message' }] });
      await sleepms(10000);
    } finally {
      await producer.disconnect();
    }
  }, 30000);
});
