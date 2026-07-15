import { after, before, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, KafkaMessage, logLevel, Producer } from 'kafkajs';
import { KafkaReceiver } from '..';

// Regression: a consumer's queue must be polled under a listenQueues filter even when it is
// database-backed, i.e. absent from the in-memory registry. The sibling listen-queues suite covers
// only the in-memory branch — its consumer uses the internal queue, which is always registered — so
// the dispatcher's separate database-backed branch is reachable only from here.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-dbq-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const suffix = Math.floor(Math.random() * 1_000_000_000);
const topic = `dbos-dbq-${suffix}`;
const queueName = `dbos-test-kafka-dbq-${suffix}`;
const NUM_MESSAGES = 4;

const seen = new Set<number>();
let resolveDone: () => void;
const done = new Promise<void>((r) => (resolveDone = r));

async function dbQueueWorkflow(_topic: string, _partition: number, message: KafkaMessage) {
  await Promise.resolve();
  seen.add(Number(message.value!.toString()));
  if (seen.size === NUM_MESSAGES) resolveDone();
}

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

suite('kafkajs-receive-listen-queues-db-backed', async () => {
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
        await dropDB(client, 'kafka_dbq_test_dbos_sys', true);
      } finally {
        await client.end();
      }

      // Persist the queue through the public API, which requires a launched instance. It lives in
      // the queues table only — nothing puts it in the in-memory registry.
      DBOS.setConfig({ name: 'kafka-dbq-test' });
      await DBOS.launch();
      const registered = await DBOS.registerQueue(queueName, { concurrency: 10 });
      assert.equal(registered.databaseBacked, true, 'the queue under test must be database-backed');

      // Start over with a cleared registry, so the consumer below is the only registration. The
      // queue row survives: destroy tears down connections, not data.
      await DBOS.shutdown({ deregister: true });

      const receiver = new KafkaReceiver(kafkaConfig);
      receiver.registerConsumer(DBOS.registerWorkflow(dbQueueWorkflow, { name: 'dbQueueWorkflow' }), topic, {
        name: 'dbQueueWorkflow',
        queueName,
        config: { groupId: `dbos-dbq-grp-${suffix}` },
      });

      // Listen only to an unrelated queue — deliberately not the consumer's.
      DBOS.setConfig({ name: 'kafka-dbq-test', listenQueues: ['dbos-test-unrelated-queue'] });
      await DBOS.launch();
    },
    { timeout: 90000 },
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
    'a database-backed consumer queue is polled despite a listenQueues filter',
    { skip: !kafkaAvailable, timeout: 60000 },
    async () => {
      await withTimeout(
        done,
        50000,
        'Timeout: the database-backed consumer queue was not polled, so its messages sat ENQUEUED',
      );
      assert.deepEqual(
        [...seen].sort((a, b) => a - b),
        Array.from({ length: NUM_MESSAGES }, (_, i) => i),
      );
    },
  );
}).catch(assert.fail);
