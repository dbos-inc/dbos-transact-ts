import { after, before, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, KafkaMessage, logLevel, Producer } from 'kafkajs';
import { KafkaReceiver } from '..';

// Regression: a Kafka consumer's queue must be polled even when listenQueues names only unrelated
// queues, else its messages would sit ENQUEUED forever.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-listen-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const kafkaReceiver = new KafkaReceiver(kafkaConfig);

const suffix = Math.floor(Math.random() * 1_000_000_000);
const topic = `dbos-listen-${suffix}`;
const NUM_MESSAGES = 5;

const seen = new Set<number>();
let resolveDone: () => void;
const done = new Promise<void>((r) => (resolveDone = r));

async function listenWorkflow(_topic: string, _partition: number, message: KafkaMessage) {
  await Promise.resolve();
  seen.add(Number(message.value!.toString()));
  if (seen.size === NUM_MESSAGES) resolveDone();
}

const registered = DBOS.registerWorkflow(listenWorkflow, { name: 'listenWorkflow' });
kafkaReceiver.registerConsumer(registered, topic, {
  name: 'listenWorkflow',
  config: { groupId: `dbos-listen-grp-${suffix}` },
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

suite('kafkajs-receive-listen-queues', async () => {
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
        await dropDB(client, 'kafka_listen_test_dbos_sys', true);
      } finally {
        await client.end();
      }

      // Listen only to an unrelated queue — deliberately NOT the internal Kafka queue.
      DBOS.setConfig({ name: 'kafka-listen-test', listenQueues: ['dbos-test-unrelated-queue'] });
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
