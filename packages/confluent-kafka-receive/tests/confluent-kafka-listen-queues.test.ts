import { after, before, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, logLevel, Producer } from 'kafkajs';
import { KafkaJS as ConfluentKafkaJS } from '@confluentinc/kafka-javascript';
import { ConfluentKafkaReceiver } from '..';

// Regression: a Kafka consumer's queue must be polled even when listenQueues names only unrelated
// queues, else its messages would sit ENQUEUED forever.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-conf-kafka-listen-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const kafkaReceiver = new ConfluentKafkaReceiver({
  clientId: 'dbos-conf-kafka-listen-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  logLevel: 0,
});

const suffix = Math.floor(Math.random() * 1_000_000_000);
const topic = `dbos-conf-listen-${suffix}`;
const NUM_MESSAGES = 5;

const seen = new Set<number>();
let resolveDone: () => void;
const done = new Promise<void>((r) => (resolveDone = r));

async function listenWorkflow(_topic: string, _partition: number, message: ConfluentKafkaJS.Message) {
  await Promise.resolve();
  seen.add(Number(message.value!.toString()));
  if (seen.size === NUM_MESSAGES) resolveDone();
}

const registered = DBOS.registerWorkflow(listenWorkflow, { name: 'confListenWorkflow' });
kafkaReceiver.registerConsumer(registered, topic, {
  name: 'confListenWorkflow',
  config: { 'group.id': `dbos-conf-listen-grp-${suffix}` },
});

async function validateKafka(config: KafkaConfig) {
  const kafka = new Kafka(config);
  const admin = kafka.admin();
  try {
    await admin.connect();
    await admin.listTopics();
    return true;
  } catch {
    return false;
  } finally {
    await admin.disconnect();
  }
}

suite('confluent-kafka-receive-listen-queues', async () => {
  const kafkaAvailable = await validateKafka(kafkaConfig);
  let producer: Producer | undefined = undefined;

  before(
    async () => {
      if (!kafkaAvailable) return;

      const kafka = new Kafka(kafkaConfig);
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], timeout: 10000 });
      await admin.disconnect();

      producer = kafka.producer();
      await producer.connect();
      for (let i = 0; i < NUM_MESSAGES; i++) {
        await producer.send({ topic, messages: [{ value: String(i) }] });
      }

      const client = new Client({ user: 'postgres', database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'conf_kafka_listen_test_dbos_sys', true);
      } finally {
        await client.end();
      }

      // Listen only to an unrelated queue — deliberately NOT the internal Kafka queue.
      DBOS.setConfig({ name: 'conf-kafka-listen-test', listenQueues: [`dbos-conf-test-unrelated-queue-${suffix}`] });
      await DBOS.launch();
    },
    { timeout: 60000 },
  );

  after(
    async () => {
      if (!kafkaAvailable) return;
      await producer?.disconnect();
      await DBOS.shutdown();
    },
    { timeout: 30000 },
  );

  await test(
    'the consumer queue is polled despite a listenQueues filter',
    { skip: !kafkaAvailable, timeout: 60000 },
    async () => {
      await withTimeout(done, 50000, 'Timeout: the consumer queue was not polled, so its messages sat ENQUEUED');
      assert.deepEqual(
        [...seen].sort((a, b) => a - b),
        Array.from({ length: NUM_MESSAGES }, (_, i) => i),
      );
    },
  );
}).catch(assert.fail);
